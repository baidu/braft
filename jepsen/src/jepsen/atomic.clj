(ns jepsen.atomic
    (:require [clojure.tools.logging :refer :all]
              [clojure.string :as str]
              [knossos.op :as op]
              [cheshire.core :as json]
              [jepsen [db :as db]
                      [client  :as client]
                      [checker   :as checker]
                      [model     :as model]
                      [generator :as gen]
                      [nemesis   :as nemesis]
                      [store     :as store]
                      [report    :as report]
                      [control :as c]
                      [tests :as tests]]
             [jepsen.checker.timeline :as timeline]))

(def atomic-bin "atomic_server")
(def atomic-path "/root/atomic")
(def peers "list://n1:8700,n2:8700,n3:8700,n4:8700,n5:8700")
(def ip_and_port "0.0.0.0:8700")

(def cas-msg-pattern
  "atomic returns following error for CAS failed"
  (re-pattern "'.*' atomic_cas failed, id: '.*' old: '.*' new: '.*'"))

(def timeout-msg-pattern
  (re-pattern "'.*' atomic_cas timedout, id: '.*' old: '.*' new: '.*'"))

(defn start!
  "start atomic_server."
  [node]
  (info node "start atomic_server")
  (c/cd atomic-path
      (c/exec :sh "jepsen_control.sh" "start")
      (c/exec :sleep 5)))

(defn stop!
  "stop atomic_server."
  [node]
  (info node "stop atomic_server")
  (c/cd atomic-path
      (c/exec :sh "jepsen_control.sh" "stop")
      (c/exec :sleep 5)))

(defn restart!
  "restart atomic_server."
  [node]
  (info node "restart atomic_server")
  (c/cd atomic-path
      (c/exec :sh "jepsen_control.sh" "restart")
      (c/exec :sleep 5)))

(defn add!
  "add atomic_server."
  [node]
  (info node "add atomic_server")
  (c/cd atomic-path
      (c/exec :sh "jepsen_control.sh" "join")
      (c/exec :sleep 5)))

(defn remove!
  "remove atomic_server."
  [node]
  (info node "remove atomic_server")
  (c/cd atomic-path
      (c/exec :sh "jepsen_control.sh" "leave")
      (c/exec :sleep 5)))

(defn db
    "atomic DB"
    []
    (reify db/DB
        (setup! [_ test node]
            (doto node
                (start!)))
        (teardown! [_ test node]
            (doto node
                (stop!)))))

(defn atomic-get!
    "get a value for id"
    [node id]
    (try
        (c/on node
            (c/su
                (c/cd atomic-path
                    (c/exec "./atomic_test"
                          :-conf peers
                          :-atomic_op "get"
                          :-atomic_id id))))
    (catch Exception e
      ; For CAS failed, we return false, otherwise, re-raise the error. 
      (if (re-matches #".*atomic_get failed.*" (str/trim (.getMessage e)))
        false
        (throw e)))))

(defn atomic-set!
    "set a value for id"
    [node id value]
    (try
        (c/on node
            (c/su
                (c/cd atomic-path
                    (c/exec "./atomic_test"
                            :-conf peers
                            :-atomic_op "set"
                            :-atomic_val value
                            :-atomic_id id))))
    (catch Exception e
      ; For CAS failed, we return false, otherwise, re-raise the error. 
      (if (re-matches #".*atomic_set failed.*" (str/trim (.getMessage e)))
        false
        (throw e)))))

(defn atomic-cas!
    "cas set a value for id"
    [node id value1 value2]
    (try
        (c/on node
            (c/su
                (c/cd atomic-path
                    (c/exec "./atomic_test"
                            :-conf peers
                            :-atomic_op "cas"
                            :-atomic_val value1
                            :-atomic_new_val value2
                            :-atomic_id id))))
    (catch Exception e
      ; For CAS failed, we return false, otherwise, re-raise the error. 
      (if (re-matches #".*atomic_cas failed.*" (str/trim (.getMessage e)))
        false
        (throw e)))))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
          (let [n node]
            ;(atomic-set! node k (json/generate-string 0))
            (assoc this :client node)))
  
  (invoke! [this test op]
           (try
             (case (:f op)  
               :read  (let [resp (-> client
                                     (atomic-get! k)
                                     (json/parse-string true))]
                        (assoc op :type :ok :value resp))
               :write (do (->> (:value op)
                               json/generate-string
                               (atomic-set! client k))
                        (assoc op :type :ok))
               
               :cas   (let [[value value'] (:value op)
                            ok?            (atomic-cas! client k
                                                          (json/generate-string value)
                                                          (json/generate-string value'))]
                        (assoc op :type (if ok? :ok :fail))))
             (catch Exception e
               (let [msg (str/trim (.getMessage e))]
                 (cond 
                   (re-matches #".*timedout.*" msg) (assoc op :type :fail :value :timed-out)
                   :else (throw e))))))
  
  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single atomic id."
  []
  (CASClient. 1 nil))

(defn mostly-small-nonempty-subset
  "Returns a subset of the given collection, with a logarithmically decreasing
  probability of selecting more elements. Always selects at least one element.

      (->> #(mostly-small-nonempty-subset [1 2 3 4 5])
           repeatedly
           (map count)
           (take 10000)
           frequencies
           sort)
      ; => ([1 3824] [2 2340] [3 1595] [4 1266] [5 975])"
  [xs]
  (-> xs
      count
      inc
      Math/log
      rand
      Math/exp
      long
      (take (shuffle xs))))

(def crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  (nemesis/node-start-stopper
    mostly-small-nonempty-subset
    (fn start [test node] (stop! node) [:killed node])
    (fn stop  [test node] (restart! node) [:restarted node])))

(def configuration-nemesis
  "A nemesis that add/remove a random node."
  (nemesis/node-start-stopper
    rand-nth
    (fn start [test node] (remove! node) [:removed node])
    (fn stop  [test node] (add! node) [:added node])))

(defn recover
  "A generator which stops the nemesis and allows some time for recovery."
  []
  (gen/nemesis
    (gen/phases
      (gen/once {:type :info, :f :stop})
      (gen/sleep 20))))

(defn read-once
  "A generator which reads exactly once."
  []
  (gen/clients
    (gen/once {:type :invoke, :f :read})))

(defn atomic-test
  "Defaults for testing atomic."
  [name opts]
  (merge tests/noop-test
         {:name (str "atomic" name)
          :db   (db)}
         opts))

(defn cas-register-nil-zero
  "A compare-and-set register, which read nil return 0"
  ([]      (model/->CASRegister 0))
  ([value] (model/->CASRegister value)))

(defn create-test
  "A generic create test."
  [name opts]
  (atomic-test (str "." name)
           (merge {:client  (cas-client)
                   :model     (cas-register-nil-zero)
                   :checker   (checker/compose {:html   timeline/html
                                                :linear checker/linearizable})
                   :ssh {:username "root"
                         :password "bcetest"
                         :strict-host-key-checking "false"}}
                   opts)))

(defn create-crash-test
  "killing random nodes and restarting them."
  []
  (create-test "crash"
               {:nemesis   crash-nemesis
                 :generator (gen/phases
                              (->> gen/cas
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                      (gen/seq
                                          (cycle [(gen/sleep 10)
                                              {:type :info :f :start}
                                              (gen/sleep 10)
                                              {:type :info :f :stop}])))
                                  (gen/time-limit 120))
                              (recover)
                              (read-once))}))

(defn create-configuration-test
  "remove and add a random node."
  []
  (create-test "configuration"
               {:nemesis   configuration-nemesis
                 :generator (gen/phases
                              (->> gen/cas
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                      (gen/seq
                                          (cycle [(gen/sleep 10)
                                              {:type :info :f :start}
                                              (gen/sleep 10)
                                              {:type :info :f :stop}])))
                                  (gen/time-limit 120))
                              (recover)
                              (read-once))}))

(defn create-partition-test
  "Cuts the network into randomly chosen halves."
  []
  (create-test "partition"
               { :nemesis   (nemesis/partition-random-halves)
                 :generator (gen/phases
                              (->> gen/cas
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                      (gen/seq
                                          (cycle [(gen/sleep 10)
                                              {:type :info :f :start}
                                              (gen/sleep 10)
                                              {:type :info :f :stop}])))
                                  (gen/time-limit 120))
                              (recover)
                              (read-once))}))

(defn create-pause-test
  "pausing random node with SIGSTOP/SIGCONT."
  []
  (create-test "pause"
               { :nemesis   (nemesis/hammer-time atomic-bin)
                 :generator (gen/phases
                              (->> gen/cas
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                      (gen/seq
                                          (cycle [(gen/sleep 10)
                                              {:type :info :f :start}
                                              (gen/sleep 10)
                                              {:type :info :f :stop}])))
                                  (gen/time-limit 120))
                              (recover)
                              (read-once))}))

(defn create-bridge-test
  "weaving the network into happy little intersecting majority rings"
  []
  (create-test "bridge"
               { :nemesis   (nemesis/partitioner (comp nemesis/bridge shuffle))
                 :generator (gen/phases
                              (->> gen/cas
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                      (gen/seq
                                          (cycle [(gen/sleep 10)
                                              {:type :info :f :start}
                                              (gen/sleep 10)
                                              {:type :info :f :stop}])))
                                  (gen/time-limit 120))
                              (recover)
                              (read-once))}))

