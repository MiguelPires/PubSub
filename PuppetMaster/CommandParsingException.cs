#region

using System;
using System.Runtime.Serialization;

#endregion

namespace PuppetMaster
{
    [Serializable]
    internal class CommandParsingException : Exception
    {
        public CommandParsingException()
        {
        }

        public CommandParsingException(string message) : base(message)
        {
        }

        public CommandParsingException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected CommandParsingException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}