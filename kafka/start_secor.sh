#! bin/bash

cd /usr/local/secor/bin
sudo java -ea -Dsecor_group=secor_backup -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.backup.properties -cp secor-0.22-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain