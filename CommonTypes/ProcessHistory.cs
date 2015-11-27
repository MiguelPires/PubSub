#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

#endregion

namespace CommonTypes
{
    public class ProcessHistory
    {
        private readonly IDictionary<int, string[]> history = new ConcurrentDictionary<int, string[]>();

        public void AddMessage(string[] message, int position)
        {
            this.history[position] = message;
        }

        public string[] GetMessage(int position)
        {
            string[] message = null;

            try
            {
                message = this.history[position];
            } catch (KeyNotFoundException)
            {
            } catch (IndexOutOfRangeException)
            {
            } catch (ArgumentOutOfRangeException)
            {
            }

            return message;
        }
    }
}