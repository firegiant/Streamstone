using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    class EventIdEntity : TableEntity
    {
        public const string RowKeyPrefix = "SS-UID-";

        public EventIdEntity()
        {}

        public EventIdEntity(Partition partition, RecordedEvent @event)
        {
            Event = @event;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.EventIdRowKey(@event.Id);
            Version = @event.Version;
        }

        public long Version
        {
            get; set;
        }

        [IgnoreProperty]
        public RecordedEvent Event
        {
            get; set;
        }
    }
}