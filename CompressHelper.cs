using System.IO.Compression;
using System.Text;

namespace BigLog.Utilities
{
    public static class CompressHelper
    {
        /// <summary>
        /// String compression https://stackoverflow.com/questions/7343465/compression-decompression-string-with-c-sharp
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static string CompressToBase64(this string data)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(data).Compress()).Replace('=', '.').Replace('+', '-').Replace('/', '_');
            // .TrimEnd("="[0]) не работает, ошибка на выходе у VS
        }
        /// <summary>
        /// String encoding without compression
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static string EncodeToBase64(this string data)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(data));
        }
        /// <summary>
        /// String decompression
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static string? DecompressFromBase64(this string? data)
        {
            if (data == null) return null;
            string incoming = data.Replace('_', '/').Replace('-', '+').Replace('.', '=');
            switch (data.Length % 4)
            {
                case 2: incoming += "=="; break;
                case 3: incoming += "="; break;
                default:
                    break;
            }
            try
            {
                return Encoding.UTF8.GetString(Convert.FromBase64String(incoming).Decompress());
            }
            catch (Exception) //
            {
                return null;
            }
        }
        /// <summary>
        /// Byte array compression
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static byte[] Compress(this byte[] data)
        {
            MemoryStream sourceStream = new(data);
            MemoryStream destinationStream = new() { Position = 0 };
            sourceStream.CompressTo(destinationStream);
            return destinationStream.ToArray();
        }
        /// <summary>
        /// Byte array decompression
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static byte[] Decompress(this byte[] data)
        {
            MemoryStream sourceStream = new(data);
            MemoryStream destinationStream = new() { Position = 0 };
            sourceStream.DecompressTo(destinationStream);
            return destinationStream.ToArray();
        }
        /// <summary>
        /// Flow compression
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="outputStream"></param>
        public static void CompressTo(this Stream stream, Stream outputStream)
        {
            using GZipStream gZipStream = new(outputStream, CompressionMode.Compress);
            stream.CopyTo(gZipStream);
            gZipStream.Flush();
        }
        /// <summary>
        /// Flow decompression
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="outputStream"></param>
        public static void DecompressTo(this Stream stream, Stream outputStream)
        {
            using GZipStream gZipStream = new(stream, CompressionMode.Decompress);
            gZipStream.CopyTo(outputStream);
        }
    }
}
