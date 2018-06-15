namespace MPI
{
    using System;

    public class Memory
    {
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public static unsafe IntPtr LoadAddress<T>(ref T value)
        {
            TypedReference reference = __makeref(value);

            return *(IntPtr*)&reference;
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public static unsafe IntPtr LoadAddressOfOut<T>(out T value)
        {
            value = default(T);
            TypedReference reference = __makeref(value);

            return *(IntPtr*)&reference;
        }
    }
}