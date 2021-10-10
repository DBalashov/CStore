using System;
using System.Runtime.InteropServices;

namespace CStore
{
    static partial class PackUnpackExtenders
    {
        static int PackKeys(this CDT[] keys, Range range, Span<byte> target)
        {
            MemoryMarshal.Cast<CDT, byte>(keys.AsSpan(range)).CopyTo(target);
            return range.Length() * 4;
        }

        static CDT[] UnpackKeys(this Span<byte> span) => MemoryMarshal.Cast<byte, CDT>(span).ToArray();
    }
}