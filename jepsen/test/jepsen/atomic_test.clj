(ns jepsen.atomic-test
  (:use jepsen.atomic
        jepsen.core
        jepsen.tests
        clojure.test
        clojure.pprint)
  (:require [clojure.string   :as str]
            [jepsen.util      :as util]
            [jepsen.os.debian :as debian]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.model     :as model]
            [jepsen.generator :as gen]
            [jepsen.nemesis   :as nemesis]
            [jepsen.store     :as store]
            [jepsen.report    :as report]
            [jepsen.core :as jepsen]))

;(deftest partition-test
;  (let [test (run!
;               (assoc
;                 noop-test
;                 :name      "atomic"
;                 :db        (db)
;                 :client    (cas-client)
;                 :model     (model/cas-register)
;                 :checker   (checker/compose {:html   timeline/html
;                                              :linear checker/linearizable})
;                 :nemesis   (nemesis/partition-random-halves)
;                 :generator (gen/phases
;                              (->> gen/cas
;                                   (gen/delay 1/2)
;                                   (gen/nemesis
;                                     (gen/seq
;                                       (cycle [(gen/sleep 10)
;                                               {:type :info :f :start}
;                                               (gen/sleep 10)
;                                               {:type :info :f :stop}])))
;                                   (gen/time-limit 120))
;                              (gen/nemesis
;                                (gen/once {:type :info :f :stop}))
;                              ;                              (gen/sleep 10)
;                              (gen/clients
;                                (gen/once {:type :invoke :f :read})))
;                :ssh {:username "root"
;                      :password "bcetest"
;                      :strict-host-key-checking "false"}))]
;    (is (:valid? (:results test)))
;    (report/linearizability (:linear (:results test)))))

(defn run-set-test!
  "Runs a test around set creation and dumps some results to the report/ dir"
  [t]
  (let [test (jepsen/run! t)]
    (is (:valid? (:results test)))
    (report/linearizability (:linear (:results test)))))

(deftest create-crash
  (run-set-test! (create-crash-test)))

(deftest create-configuration
  (run-set-test! (create-configuration-test)))

(deftest create-pause
  (run-set-test! (create-pause-test)))

(deftest create-partition
  (run-set-test! (create-partition-test)))

(deftest create-bridge
  (run-set-test! (create-bridge-test)))

