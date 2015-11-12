using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Broker
{
    public class CommandQueue
    {
        private readonly IDictionary<int, string[]> commands;

        public CommandQueue()
        {
            this.commands = new ConcurrentDictionary<int, string[]>();
        }

        public void AddCommand(string[] command, int sequenceNumber)
        {
            this.commands[sequenceNumber] = command;
        }

        public string[] GetCommand(int sequenceNumber)
        {
            string[] command;

            if (this.commands.TryGetValue(sequenceNumber, out command))
            {
                return command;
            }

            return null;
        }

    }
}