# jepsen.atomic

A Clojure library designed to ... well, that part is up to you.

## Usage

1. install apache-maven sun-java8 lein-2.6.1 
2. prepare 6 hosts, 1 control host, 5 test hosts
    test hosts named n1, n2, n3, n4, n5
    add n1 n2 n3 n4 n5 to /etc/hosts
3. make control host can ssh to test hosts
    make password root:root
    ~/.ssh/known_hosts setting
    /etc/ssh/sshd_config allow: PermitRootLogin yes
    /etc/sudoers disable: #Defaults    requiretty
6. lein deps && lein test

[NOTE] iptables command invalid in jepsen/src/net.clj, remove -w. then lein install
most code copy from elasticsearch and logcabin

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
