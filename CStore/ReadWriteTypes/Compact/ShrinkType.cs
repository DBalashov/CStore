using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    #region CompactedResult

    readonly ref struct CompactedResult
    {
        public readonly CompactType       KeyType;
        public readonly CompactedResultItem Indexes;
        public readonly CompactedResultItem Values;

        internal CompactedResult(CompactType       keyType,
                                 CompactedResultItem indexes,
                                 CompactedResultItem values)
        {
            KeyType = keyType;
            Indexes = indexes;
            Values  = values;
        }
    }

    readonly ref struct CompactedResultItem
    {
        public readonly Span<byte> Body;
        public readonly int        ElementCount;

        public CompactedResultItem(Span<byte> body, int elementCount)
        {
            Body         = body;
            ElementCount = elementCount;
        }

        public override string ToString() => $"[{ElementCount}] {Body.Length} bytes";
    }

    #endregion

    static class ShrinkTypeCommonExtenders
    {
        internal static byte[] Combine(this CompactedResult packed)
        {
            var buff = new byte[(2 + 4 + 4 + packed.Indexes.Body.Length + packed.Values.Body.Length)];
            var span = buff.AsSpan();

            span[0] = (byte)CompactKind.Dictionary;
            span[1] = (byte)packed.KeyType;
            span    = span.Slice(2);

            BitConverter.TryWriteBytes(span, packed.Indexes.ElementCount);
            span = span.Slice(4);

            BitConverter.TryWriteBytes(span, packed.Values.ElementCount);
            span = span.Slice(4);

            packed.Indexes.Body.CopyTo(span);
            packed.Values.Body.CopyTo(span.Slice(packed.Indexes.Body.Length));

            return buff;
        }

        internal static Span<byte> CompactToShort(this int[] data)
        {
            var target = new ushort[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (ushort)(data[i] & 0xFFFF);
            return MemoryMarshal.Cast<ushort, byte>(target);
        }

        internal static Span<byte> CompactToByte(this int[] data)
        {
            var target = new byte[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (byte)(data[i] & 0xFF);
            return target;
        }

        internal static Span<byte> CompactToInt(this int[] data) => MemoryMarshal.Cast<int, byte>(data);
    }
}