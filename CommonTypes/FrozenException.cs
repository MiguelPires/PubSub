#region

using System;
using System.Runtime.Serialization;

#endregion

namespace CommonTypes
{
    [Serializable]
    public class FrozenException : Exception
    {
        public FrozenException()
        {
        }

        public FrozenException(string message) : base(message)
        {
        }

        public FrozenException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected FrozenException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}