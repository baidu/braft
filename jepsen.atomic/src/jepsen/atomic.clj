(ns jepsen.atomic
    (:require [clojure.tools.logging :refer :all]
              [clojure.string :as str]
              [knossos.op :as op]
              [cheshire.core :as json]
              [jepsen [db :as db]
                      [client  :as client]
                      [generator :as gen]
                      [control :as c]
                      [tests :as tests]]))

(def atomic-path "/root/atomic")
(def peers "list://10.75.27.19:8700,10.75.27.20:8700,10.75.27.25:8700,10.75.27.27:8700,10.75.27.28:8700")
(def ip_and_port "0.0.0.0:8700")

(def cas-msg-pattern
  "atomic returns following error for CAS failed"
  (re-pattern "'.*' atomic_cas failed, id: '.*' old: '.*' new: '.*'"))

(def timeout-msg-pattern
  (re-pattern "'.*' atomic_cas timedout, id: '.*' old: '.*' new: '.*'"))

(defn db
    "atomic DB"
    []
    (reify db/DB
        (setup! [_ test node]
            (info node "installing atomic")
            (c/cd atomic-path
                (c/exec :sh "start.sh")
                (c/exec :sleep 5)))
        (teardown! [_ test node]
            (info node "tearing down atomic")
            (c/cd atomic-path
                (c/exec :sh "stop.sh")))))


(defn atomic-get!
    "get a value for id"
    [node id]
    (try
        (c/on node
            (c/su
                (c/cd atomic-path
                    (c/exec "./atomic_client"
                          :-cluster_ns peers
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
                    (c/exec "./atomic_client"
                            :-cluster_ns peers
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
                    (c/exec "./atomic_client"
                            :-cluster_ns peers
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

