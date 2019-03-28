repro:
# clean slate

        ~/Downloads/confluent-5.1.2/bin/confluent stop
        rm -rfv /tmp/{confl*,kafka*}

# start kafka

        ~/Downloads/confluent-5.1.2/bin/confluent start kafka

# compile

        mvn clean package assembly:single

# produce data

        java -cp target/suppress-demo-1.0-SNAPSHOT.jar org.vvcephei.demo.Producer PT1H 100

# start app

        (in term 1) java -cp target/suppress-demo-1.0-SNAPSHOT.jar org.vvcephei.demo.App clean

# (second terminal) check the results (keep re-running this until you see some results)

        (in term 2) java -cp target/suppress-demo-1.0-SNAPSHOT.jar org.vvcephei.demo.Analyzer

# stop the app

        (in term 1) Ctrl-C

# start the app again

        (in term 1) java -cp target/suppress-demo-1.0-SNAPSHOT.jar org.vvcephei.demo.App restart

# (second terminal) check the results (re-run as desired)

        (in term 2) java -cp target/suppress-demo-1.0-SNAPSHOT.jar org.vvcephei.demo.Analyzer

After running the last Analyzer step several times, I start seeing "invalids", meaning that one key-with-window-start pair appears in multiple results, which is unexpected.

