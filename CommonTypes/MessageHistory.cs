#region

using System.Collections.Concurrent;
using System.Collections.Generic;

#endregion

namespace CommonTypes
{
    public class MessageHistory
    {
        // maps a site's name to a table that maps processes' names to a list of messages
        private readonly IDictionary<string, IDictionary<string, ProcessHistory>> _publicationHistory =
            new ConcurrentDictionary<string, IDictionary<string, ProcessHistory>>();

        private readonly IDictionary<string, ProcessHistory> SubscriptionHistory =
            new ConcurrentDictionary<string, ProcessHistory>();

        /// <summary>
        ///     Stores the publication
        /// </summary>
        /// <param name="siteName"></param>
        /// <param name="message"></param>
        public void StorePublication(string siteName, string[] message)
        {
            lock (this._publicationHistory)
            {
                string publisher = message[0];
                int seqNo = int.Parse(message[4]);

                // gets or creates the history of messages for the given site
                IDictionary<string, ProcessHistory> procToHistory;
                if (!this._publicationHistory.TryGetValue(siteName, out procToHistory))
                {
                    procToHistory = new ConcurrentDictionary<string, ProcessHistory>();
                    this._publicationHistory[siteName] = procToHistory;
                }

                // gets or creates the history of messages for the given publisher
                ProcessHistory procHistory;
                if (!procToHistory.TryGetValue(publisher, out procHistory))
                {
                    procHistory = new ProcessHistory();
                    procToHistory[publisher] = procHistory;
                }

                // adds the message to the process' history
                procHistory.AddMessage(message, seqNo);
            }
        }

        /// <summary>
        ///     Retrieves the publication
        /// </summary>
        /// <param name="siteName"></param>
        /// <param name="publisher"></param>
        /// <param name="sequenceNumber"></param>
        /// <returns></returns>
        public string[] GetPublication(string siteName, string publisher, int sequenceNumber)
        {
            // gets the history of messages for the given site and publisher

            IDictionary<string, ProcessHistory> procToHistory;
            ProcessHistory procHistory;
            if (this._publicationHistory.TryGetValue(siteName, out procToHistory) &&
                procToHistory.TryGetValue(publisher, out procHistory))
            {
                return procHistory.GetMessage(sequenceNumber);
            }

            return null;
        }

        public void StoreSubscription(string subscriber, string topic, int sequenceNumber)
        {
            // gets or creates the history of messages for the given subscriber
            ProcessHistory processHistory;
            if (!this.SubscriptionHistory.TryGetValue(subscriber, out processHistory))
            {
                processHistory = new ProcessHistory();
                this.SubscriptionHistory[subscriber] = processHistory;
            }

            // adds the message to the process' history
            processHistory.AddMessage(new[] {subscriber, topic, sequenceNumber.ToString()}, sequenceNumber);
        }

        public string[] GetSubscription(string subscriber, int sequenceNumber)
        {
            // gets the history of messages for the subscriber
            ProcessHistory processHistory;
            if (this.SubscriptionHistory.TryGetValue(subscriber, out processHistory))
            {
                return processHistory.GetMessage(sequenceNumber);
            }

            return null;
        }
    }
}