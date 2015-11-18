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

        public string[] GetCommandAndRemove(int sequenceNumber)
        {
            string[] command;

            if (this.commands.TryGetValue(sequenceNumber, out command))
            { 
                commands.Remove(sequenceNumber);
                return command;
            }

            return null;
        }

        public ICollection<int> GetSequenceNumbers()
        {
            return commands.Keys;
        }

    }
}