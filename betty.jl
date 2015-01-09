#!/usr/bin/env julia

#
# Reading from multiple sockets
# This version uses Julia Tasks instead of zmq.Poller()
#

using ZMQ

# Prepare our context and sockets
context = Context()

# Connect to weather server
subscriber = Socket(context, SUB)
ZMQ.connect(subscriber, "tcp://localhost:5556")
ZMQ.set_subscribe(subscriber, "10001")

# Connect to task ventilator
receiver = Socket(context, PULL)
ZMQ.connect(receiver, "tcp://localhost:5557")

# Socket to send messages to
sender = Socket(context, PUSH)
ZMQ.connect(sender, "tcp://localhost:5558")

###################

function wuclient()

    println("Collecting updates from weather server...")

    # Process updates
    update = 0
    total_temp = 0

    while true
        message = bytestring(ZMQ.recv(subscriber))
        zipcode, temperature, relhumidity = split(message)
        total_temp += int(temperature)
        update += 1
        
        avg_temp = int(total_temp / update)
        
        println("Average temperature for zipcode 10001 was $(avg_temp)F")
    end
end

###################

function taskwork()

    # Process tasks forever
    while true
        s = bytestring(ZMQ.recv(receiver))
        
        # Simple progress indicator for the viewer
        write(STDOUT, ".")
        flush(STDOUT)
        
        # Do the work
        sleep(int(s)*0.001)

        # Send results to sink
        ZMQ.send(sender, b"")
    end
end

###################

function waitloop()
    @async wuclient()
    @async taskwork()
    while true
        try
            wait()
        catch e
            # send interrupts (user SIGINT) to the code-execution task
            if isa(e, InterruptException)
                # clean up stuff you!?
                println("InterruptException")
            else
                rethrow()
            end
        end
    end
end

# Process messages from multiple sockets
waitloop()