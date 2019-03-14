using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    /// <summary>
    /// Represents table partition. Virtual partitions are created 
    /// by using <c>`|`</c> split separator in a key.
    /// </summary>
    public sealed class Partition
    {
        static readonly char Separator = '|';

        /// <summary>
        /// The table in which this partition resides
        /// </summary>
        public readonly CloudTable Table;

        /// <summary>
        /// The partition key
        /// </summary>
        public readonly string PartitionKey;

        /// <summary>
        /// The row key prefix. Will be non-empty only for virtual partitions
        /// </summary>
        public readonly string RowKeyPrefix;

        /// <summary>
        /// The full key of this partition
        /// </summary>
        public readonly string Key;

        /// <summary>
        /// The stream row key for internal use.
        /// </summary>
        internal readonly string StreamRowKey;

        /// <summary>
        /// Initializes a new instance of the <see cref="Partition"/> class.
        /// </summary>
        /// <param name="table">The cloud table.</param>
        /// <param name="partitionKey">The partition's key.</param>
        public Partition(CloudTable table, string partitionKey)
        {
            Requires.NotNull(table, nameof(table));
            Requires.NotNullOrEmpty(partitionKey, nameof(partitionKey));

            if (partitionKey.Contains(Separator))
            {
                throw new ArgumentException("Partition key cannot contain virtual partition separator", nameof(partitionKey));
            }

            Table = table;

            PartitionKey = partitionKey;
            RowKeyPrefix = String.Empty;
            Key = partitionKey;
            StreamRowKey = String.Concat(RowKeyPrefix, StreamEntity.FixedRowKey);
        }

        /// <summary>
        /// Creates virtual partition using provided partition key and row key prefix.
        /// </summary>
        /// <param name="table">The cloud table.</param>
        /// <param name="partitionKey">The partition's own key.</param>
        /// <param name="rowKeyPrefix">The row's key prefix.</param>
        public Partition(CloudTable table, string partitionKey, string rowKeyPrefix)
        {
            Requires.NotNull(table, nameof(table));
            Requires.NotNullOrEmpty(partitionKey, nameof(partitionKey));
            Requires.NotNullOrEmpty(rowKeyPrefix, nameof(rowKeyPrefix));

            if (partitionKey.Contains(Separator))
            {
                throw new ArgumentException("Partition key cannot contain virtual partition separator", nameof(partitionKey));
            }

            Table = table;

            PartitionKey = partitionKey;
            RowKeyPrefix = rowKeyPrefix + Separator;

            Key = String.Concat(partitionKey, Separator, rowKeyPrefix);
            StreamRowKey = String.Concat(RowKeyPrefix, StreamEntity.FixedRowKey);
        }

        internal string EventVersionRowKey(long version) => String.Format("{0}{1}{2:d19}", RowKeyPrefix, EventEntity.RowKeyPrefix, version);

        internal string EventIdRowKey(string id) => String.Concat(RowKeyPrefix, EventIdEntity.RowKeyPrefix, id);

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ToString() => String.Concat(Table.Name, ".", Key);
    }
}
