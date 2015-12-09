# SESDAD
**Author:** Miguel Pires <br/>
**Email:** miguel.pires@tecnico.ulisboa.pt

### Configuration file

The configuration file 'master.config' is located in the root directory (where it
should remain in order for the program to execute). This particular config file
requires three sites (i.e., three consoles if we're testing it locally) and launches
three brokers at each site, two subscribers and two publishers. The network structure
looks like this:

				site0
				/   \
			 site1	site2


There are two publishers in the site0 and two subscribers in site1 and site2 (one
in each).

### Launching consoles

To launch a "slave" site just type the following in a shell in the root directory 
(the SESDAD folder):

``` bash
$ PuppetMaster/bin/Debug/PuppetMaster.exe site1
```

To launch a "master" site just type the following in a shell in the root directory 
(the SESDAD folder):

``` bash
$ PuppetMaster/bin/Debug/PuppetMaster.exe -m site0
```

The presence of the '-m' flag indicates the site where the PuppetMaster"Master" 
should be located. Any other site will have a PuppetMaster"Slave".

### Inserting commands

An example script 'script.txt' is also located in the root directory and subscribes the two
subscribers, subscriber2 and subscriber1, to topics '/p/\*' and '/p/a', respectively. Then
the publishers, publisher00 and publisher01, publish events on the topics '/p/a' and '/p/b'
(20 events each). The point of this is to demonstrate the working of hierachical filtering.
The subscriber2 will receive every event (about 80) and subscriber2 (about 40).
It is **important** to notice that they might not receive every event since there may be no delay
between the "subscribe" and "publish" commands, so the publishers might miss an arbitrary
ammount of initial publications (it's usual not much but it fluctuates).
After this, the PuppetMaster waits, sends a 'Status' request, unsubscribes from one topic and
sends another 'Status' request.

NOTE: Please give enough time for the system to start-up (just a few seconds) or some commands
might not be delivered since the services aren't all up and running. The system will be ready when
every process console says "Running a <process-name> at <url> - <site>". Brokers also have to
display two "Received sibling ..." messages that mean the broker has been informed of who are it's
neighbouring brokers (the other brokers in it's site).

### GUI

For the PuppetMasterMaster UI, there is an input box for individual commands, 
another for batch commands and a log for the triggered events. Wrong commands 
should not be problematic since the system checks for mistakes and writes them 
in the log in red color (as it does with correct commands but in standard black color).

For the PuppetMasterSlaves UI, there is only a log of events observed in that site. 
There is no way to interact with the system through these windows.

NOTE: If you want to use 'Enter' to insert a newline when writing batch commands 
by hand, then use SHIFT-Enter. Pressing plain Enter sends the commands as if you 
clicked the 'Send' button.


Thank you.
