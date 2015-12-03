#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

#endregion

namespace Broker
{
    [Serializable]
    public class MessageQueue
    {
        private readonly IDictionary<int, string[]> _messages = new ConcurrentDictionary<int, string[]>();

        public void Add(string[] message, int sequenceNumber)
        {
            this._messages[sequenceNumber] = message;
        }

        public string[] GetAndRemove(int sequenceNumber)
        {
            string[] message;

            if (this._messages.TryGetValue(sequenceNumber, out message))
            {
                this._messages.Remove(sequenceNumber);
                return message;
            }

            return null;
        }

        public string[] GetFirstAndRemove()
        {
            return GetAndRemove(this._messages.Keys.Min());
        }

        public ICollection<int> GetSequenceNumbers()
        {
            return this._messages.Keys;
        }

        public int GetCount()
        {
            return this._messages.Count;
        }
    }
}