#!/bin/bash

/etc/alternatives/jre_16/bin/java -Xms1G -Xmx1G -cp /home/jalien/alien-cs.jar:/home/jalien/spooler.jar -DAliEnConfig=/home/jalien/.alien/config spooler.Main &> /home/jalien/epn2eos_logs/$(hostname)/service.log

