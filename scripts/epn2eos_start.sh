#!/bin/bash

if [ ! -d "/home/jalien/epn2eos_logs/$(hostname)" ]
then
  mkdir /home/jalien/epn2eos_logs/$(hostname)
fi

XRD_WRITERECOVERY=false /etc/alternatives/jre_16/bin/java -Xms1G -Xmx1G -cp /home/jalien/alien-cs.jar:/home/jalien/spooler_v.1.5.jar -DAliEnConfig=/home/jalien/.alien/config spooler.Main &> /home/jalien/epn2eos_logs/$(hostname)/service.log
