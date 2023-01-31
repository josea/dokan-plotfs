using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Management;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using DokanNet;
using DokanNet.Logging;
using static DokanNet.FormatProviders;
using FileAccess = DokanNet.FileAccess;

namespace DokanNetMirror
{
    internal class Mirror : IDokanOperations
    {
        private readonly string[] paths;
        private readonly string basePath;
        private readonly string baseSeparator = "$$$";
        private const FileAccess DataAccess = FileAccess.ReadData | FileAccess.WriteData | FileAccess.AppendData |
                                              FileAccess.Execute |
                                              FileAccess.GenericExecute | FileAccess.GenericWrite |
                                              FileAccess.GenericRead;

        private const FileAccess DataWriteAccess = FileAccess.WriteData | FileAccess.AppendData |
                                                   FileAccess.Delete |
                                                   FileAccess.GenericWrite;

        private readonly ILogger _logger;

        public Mirror(ILogger logger, string[] paths, string basePath)
        {
            foreach (var path in paths)
            {
                if (!Directory.Exists(path))
                    throw new ArgumentException(nameof(path));
            }
            _logger = logger;
            this.paths = paths;
            this.basePath = basePath; 
        }

        //protected string GetPath(string fileName)
        //{
        //    return path + fileName;
        //}

        private IList<FileInformation> GetReadFilePaths(string fileName)
        {
            var result = new List<FileInformation>();
            
            foreach (var path in paths)
            {
                var files =
                    new DirectoryInfo(path)
                   // .EnumerateFileSystemInfos()
                  .GetFiles()
                  .Where(finfo => finfo.Name.ToLower() == fileName.TrimStart('\\').ToLower())
                  .Select(finfo => new FileInformation
                  {
                      Attributes = finfo.Attributes,
                      CreationTime = finfo.CreationTime,
                      LastAccessTime = finfo.LastAccessTime,
                      LastWriteTime = finfo.LastWriteTime,
                      Length = (finfo as FileInfo)?.Length ?? 0,
                      FileName = finfo.FullName                    
                  });

                result.AddRange(files);
            }
            return result; 
        }

        protected Tuple<string,string>[] GetPaths(string fileName, bool allPaths, bool forceAllPaths = false)
        {
            // if it is an object that is in one of the base folders, then only return that path.
            // eg: \\dir1$$$file1.txt (a file1.txt file in c:\scratch\base\dir1\
            //if (fileName.Contains(baseSeparator)) {
            //    var baseToSearch = fileName.Substring(0, fileName.IndexOf(baseSeparator)).TrimStart('\\');
            //    var baseFileName = fileName.Substring(fileName.IndexOf(baseSeparator) + baseSeparator.Length);
            //    foreach ( var path in paths)
            //    {
            //        if ( path.Substring(basePath.Length).TrimEnd('\\') == baseToSearch)
            //        {
            //            return new Tuple<string, string>[] { Tuple.Create(path, path + baseFileName) };
            //        }
            //    }
            //}

            if (!forceAllPaths)
            {
                // assumes filenames are unique...
                foreach (var path in paths)
                {
                    if ((new DirectoryInfo(path)
                      .EnumerateFileSystemInfos()
                      .Where(finfo => finfo.Name.ToLower() == fileName.TrimStart('\\').ToLower())
                      .Any()))
                    {
                        return new Tuple<string, string>[] { Tuple.Create(path, path + fileName.TrimStart('\\')) };
                    }
                }
            }

            if (allPaths)
            {
                var _paths = new List<Tuple<string, string>>();
                foreach (var path in paths) _paths.Add(Tuple.Create(path, path + fileName));

                return _paths.ToArray();
            }
            else
            {
                return new Tuple<string, string>[] { Tuple.Create(paths[0], paths[0] + fileName.TrimStart('\\')) };
            }
            
        }



        protected NtStatus Trace(string method, string fileName, IDokanFileInfo info, NtStatus result,
            params object[] parameters)
        {
#if TRACE
            var extraParameters = parameters != null && parameters.Length > 0
                ? ", " + string.Join(", ", parameters.Select(x => string.Format(DefaultFormatProvider, "{0}", x)))
                : string.Empty;

            _logger.Debug(DokanFormat($"{method}('{fileName}', {info}{extraParameters}) -> {result}"));
#endif

            return result;
        }

        private NtStatus Trace(string method, string fileName, IDokanFileInfo info,
            FileAccess access, FileShare share, FileMode mode, FileOptions options, FileAttributes attributes,
            NtStatus result)
        {
#if TRACE
            _logger.Debug(
                DokanFormat(
                    $"{method}('{fileName}', {info}, [{access}], [{share}], [{mode}], [{options}], [{attributes}]) -> {result}"));
#endif

            return result;
        }

        protected static Int32 GetNumOfBytesToCopy(Int32 bufferLength, long offset, IDokanFileInfo info, FileStream stream)
        {
            if (info.PagingIo)
            {
                var longDistanceToEnd = stream.Length - offset;
                var isDistanceToEndMoreThanInt = longDistanceToEnd > Int32.MaxValue;
                if (isDistanceToEndMoreThanInt) return bufferLength;
                var distanceToEnd = (Int32)longDistanceToEnd;
                if (distanceToEnd < bufferLength) return distanceToEnd;
                return bufferLength;
            }
            return bufferLength;
        }

        #region Implementation of IDokanOperations

        public NtStatus CreateFile(string fileName, FileAccess access, FileShare share, FileMode mode,
            FileOptions options, FileAttributes attributes, IDokanFileInfo info)
        {
           
            var result = DokanResult.Success;
            foreach (var pathTuple in GetPaths(fileName, false)) 
            {
                var filePath = pathTuple.Item2;
                var basePath = pathTuple.Item1;
                
                //var filePath = GetPath(fileName);

                if (info.IsDirectory)
                {
                    //TODO: in the original code, not sure what it does.
                    //TODO: I think it just checks for errors (access, exists, etc).
                    try
                    {
                        switch (mode)
                        {
                            case FileMode.Open:
                                if (!Directory.Exists(filePath))
                                {
                                    try
                                    {
                                        if (!File.GetAttributes(filePath).HasFlag(FileAttributes.Directory))
                                            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                                attributes, DokanResult.NotADirectory);
                                    }
                                    catch (Exception)
                                    {
                                        return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                            attributes, DokanResult.FileNotFound);
                                    }
                                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                        attributes, DokanResult.PathNotFound);
                                }

                                new DirectoryInfo(filePath).EnumerateFileSystemInfos().Any();
                                // you can't list the directory
                                break;

                            case FileMode.CreateNew:
                                if (Directory.Exists(filePath))
                                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                        attributes, DokanResult.FileExists);

                                try
                                {
                                    File.GetAttributes(filePath).HasFlag(FileAttributes.Directory);
                                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                        attributes, DokanResult.AlreadyExists);
                                }
                                catch (IOException)
                                {
                                }

                                // Directory.CreateDirectory(GetPath(fileName));
                                break;
                        }
                    }
                    catch (UnauthorizedAccessException)
                    {
                        return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                            DokanResult.AccessDenied);
                    }
                }
                else
                {
                    var pathExists = true;
                    var pathIsDirectory = false;

                    var readWriteAttributes = (access & DataAccess) == 0;
                    var readAccess = (access & DataWriteAccess) == 0;

                    try
                    {
                        pathExists = (Directory.Exists(filePath) || File.Exists(filePath));
                        pathIsDirectory = pathExists ? File.GetAttributes(filePath).HasFlag(FileAttributes.Directory) : false;
                    }
                    catch (IOException)
                    {
                    }

                    switch (mode)
                    {
                        case FileMode.Open:

                            if (pathExists)
                            {
                                // check if driver only wants to read attributes, security info, or open directory
                                if (readWriteAttributes || pathIsDirectory)
                                {
                                    if (pathIsDirectory && (access & FileAccess.Delete) == FileAccess.Delete
                                        && (access & FileAccess.Synchronize) != FileAccess.Synchronize)
                                        //It is a DeleteFile request on a directory
                                        return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                            attributes, DokanResult.AccessDenied);

                                    info.IsDirectory = pathIsDirectory;
                                    info.Context = new object();
                                    // must set it to something if you return DokanError.Success

                                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                        attributes, DokanResult.Success);
                                }
                            }
                            else
                            {
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                    DokanResult.FileNotFound);
                            }
                            break;

                        case FileMode.CreateNew:
                            if (pathExists)
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                    DokanResult.FileExists);
                            break;

                        case FileMode.Truncate:
                            if (!pathExists)
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                    DokanResult.FileNotFound);
                            break;
                    }

                    try
                    {
                        info.Context = new FileStream(filePath, mode,
                            readAccess ? System.IO.FileAccess.Read : System.IO.FileAccess.ReadWrite, share, 4096, options);

                        if (pathExists && (mode == FileMode.OpenOrCreate
                                           || mode == FileMode.Create))
                            result = DokanResult.AlreadyExists;

                        bool fileCreated = mode == FileMode.CreateNew || mode == FileMode.Create || (!pathExists && mode == FileMode.OpenOrCreate);
                        if (fileCreated)
                        {
                            FileAttributes new_attributes = attributes;
                            new_attributes |= FileAttributes.Archive; // Files are always created as Archive
                                                                      // FILE_ATTRIBUTE_NORMAL is override if any other attribute is set.
                            new_attributes &= ~FileAttributes.Normal;
                            File.SetAttributes(filePath, new_attributes);

                          

                        }
                        //TODO: closing the filestream as the file will be split when it 'overflows' one disk.
                        ((FileStream)info.Context).Close();
                        ((FileStream)info.Context).Dispose();
                        info.Context = null;
                    }
                    catch (UnauthorizedAccessException) // don't have access rights
                    {
                        if (info.Context is FileStream fileStream)
                        {
                            // returning AccessDenied cleanup and close won't be called,
                            // so we have to take care of the stream now
                            fileStream.Dispose();
                            info.Context = null;
                        }
                        return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                            DokanResult.AccessDenied);
                    }
                    catch (DirectoryNotFoundException)
                    {
                        return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                            DokanResult.PathNotFound);
                    }
                    catch (Exception ex)
                    {
                        var hr = (uint)Marshal.GetHRForException(ex);
                        switch (hr)
                        {
                            case 0x80070020: //Sharing violation
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                    DokanResult.SharingViolation);
                            default:
                                throw;
                        }
                    }
                }

            }
            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                result);
        }

        public void Cleanup(string fileName, IDokanFileInfo info)
        {
#if TRACE
            if (info.Context != null)
                Console.WriteLine(DokanFormat($"{nameof(Cleanup)}('{fileName}', {info} - entering"));
#endif

            (info.Context as FileStream)?.Dispose();
            info.Context = null;

            if (info.DeleteOnClose)
            {
                if (info.IsDirectory)
                {
                    Directory.Delete(ResolveFileNamePath(fileName));
                }
                else
                {
                    //File.Delete(ResolveFileNamePath(fileName));
                    foreach(var file in GetReadFilePaths(fileName))
                    {
                        File.Delete(file.FileName);
                    }

                }
            }
            Trace(nameof(Cleanup), fileName, info, DokanResult.Success);
        }

        public void CloseFile(string fileName, IDokanFileInfo info)
        {
#if TRACE
            if (info.Context != null)
                Console.WriteLine(DokanFormat($"{nameof(CloseFile)}('{fileName}', {info} - entering"));
#endif

            (info.Context as FileStream)?.Dispose();
            info.Context = null;
            Trace(nameof(CloseFile), fileName, info, DokanResult.Success);
            // could recreate cleanup code here but this is not called sometimes
        }

        public NtStatus ReadFile(string fileName, byte[] buffer, out int bytesRead, long offset, IDokanFileInfo info)
        {
            if (info.Context == null) // memory mapped read
            {
             //   using (var log = new StreamWriter(new FileStream("log.txt", FileMode.Append)))
                {
                    var files = GetReadFilePaths(fileName);
                    var totalBytesToRead = buffer.Length;
                    bytesRead = 0;
               //     log.WriteLine($"{fileName}\t{offset}\t\t\t{totalBytesToRead}");
                    foreach (var file in files)
                    {                        
                        if (offset >= file.Length)
                        {
                            offset -= file.Length;
                            continue;
                        }
                        var bytesToRead = buffer.Length - bytesRead;
                        if (bytesToRead + offset > file.Length)
                        {
                            bytesToRead = (int)(file.Length - offset);
                        }
                       // log.WriteLine($"\t{file.FullName}\t{offset}\t{file.Length}\t{totalBytesToRead}\t{bytesToRead}");
                        var resolvedFileName = file.FileName;
                        using (var stream = new FileStream(resolvedFileName, FileMode.Open, System.IO.FileAccess.Read))
                        {
                            stream.Position = offset;
                            var bRead = stream.Read(buffer, bytesRead, bytesToRead);
                            bytesRead += bRead;
                            //offset -= bRead;
                            offset = 0; 
                        }
                        if (totalBytesToRead == bytesRead) break;

                    }
                }
            }
            else // normal read
            {
                var stream = info.Context as FileStream;
                lock (stream) //Protect from overlapped read
                {
                    stream.Position = offset;
                    bytesRead = stream.Read(buffer, 0, buffer.Length);
                }
            }
            return Trace(nameof(ReadFile), fileName, info, DokanResult.Success, "out " + bytesRead.ToString(),
                offset.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus WriteFile(string fileName, byte[] buffer, out int bytesWritten, long offset, IDokanFileInfo info)
        {
            var append = offset == -1;
            var totalBytesToCopy = buffer.Length;
            int bufferOffset = 0;
            bool copyFinished = false; 
            bytesWritten = 0;
            if (info.Context == null || true )
            {
                //var filepath = ResolveFileNamePath(fileName);
                var paths = GetPaths(fileName,true , true);
                var disk = GetDisksInfo(false);
                append = true;

                foreach (var path in paths)
                {
                    var basepath = path.Item1;
                    var filepath = path.Item2;
                    var diskfree = disk[basepath].AvailableFreeSpace;
                    //diskfree = 20000; // test;

                    if (diskfree == 0) continue; // do next disk.

                    using (var stream = new FileStream(filepath, append ? FileMode.Append : FileMode.Open, System.IO.FileAccess.Write))
                    {
                        if (!append) // Offset of -1 is an APPEND: https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-writefile
                        {
                            stream.Position = offset;
                        }
                        var bytesToCopy = GetNumOfBytesToCopy(totalBytesToCopy, offset, info, stream);
                        if ( bytesToCopy > diskfree)
                        {
                            bytesToCopy = (int) diskfree; 
                        } else
                        { copyFinished = true;  }
                        stream.Write(buffer, bufferOffset, bytesToCopy);
                        bytesWritten += bytesToCopy;

                        totalBytesToCopy -= bytesToCopy;
                        bufferOffset += bytesToCopy;
                    }
                    if (copyFinished ) break;
                }
            }
            else
            {
                var stream = info.Context as FileStream;
                lock (stream) //Protect from overlapped write
                {
                    if (append)
                    {
                        if (stream.CanSeek)
                        {
                            stream.Seek(0, SeekOrigin.End);
                        }
                        else
                        {
                            bytesWritten = 0;
                            return Trace(nameof(WriteFile), fileName, info, DokanResult.Error, "out " + bytesWritten,
                                offset.ToString(CultureInfo.InvariantCulture));
                        }
                    }
                    else
                    {
                        stream.Position = offset;
                    }
                    var bytesToCopy = GetNumOfBytesToCopy(buffer.Length, offset, info, stream);
                    stream.Write(buffer, 0, bytesToCopy);
                    bytesWritten = bytesToCopy;
                }
            }
            return Trace(nameof(WriteFile), fileName, info, DokanResult.Success, "out " + bytesWritten.ToString(),
                offset.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus FlushFileBuffers(string fileName, IDokanFileInfo info)
        {
            try
            {
                ((FileStream)(info.Context)).Flush();
                return Trace(nameof(FlushFileBuffers), fileName, info, DokanResult.Success);
            }
            catch (IOException)
            {
                return Trace(nameof(FlushFileBuffers), fileName, info, DokanResult.DiskFull);
            }
        }

        /// <summary>
        /// In the multiple Paths look for the specific file and returns it. 
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        private string ResolveFileNamePath(string fileName)
        {
            foreach (var pathTuple in GetPaths(fileName,false))
            {
                var filePath = pathTuple.Item2;
                var basePath = pathTuple.Item1.Substring(this.basePath.Length).TrimEnd('\\');

                if (File.Exists(filePath) || Directory.Exists(filePath))
                {
                    return filePath; 
                }
            }
            throw new Exception("Filename not found");
        }

        public NtStatus GetFileInformation(string fileName, out FileInformation fileInfo, IDokanFileInfo info)
        {
            //throw new NotImplementedException();
            // may be called with info.Context == null, but usually it isn't
            // fileInfo = new FileInformation();
            //foreach (var pathTuple in GetPaths(fileName))
            //{
            //    var filePath = pathTuple.Item2;
            //    var basePath = pathTuple.Item1.Substring(this.basePath.Length).TrimEnd('\\');

            //var filePath = GetPaths( fileName);
            var filePath = ResolveFileNamePath(fileName); 
                FileSystemInfo finfo = new FileInfo(filePath);
            if (!finfo.Exists)
            {
                finfo = new DirectoryInfo(filePath);
                fileInfo = new FileInformation
                {
                    FileName = fileName,
                    Attributes = finfo.Attributes,
                    CreationTime = finfo.CreationTime,
                    LastAccessTime = finfo.LastAccessTime,
                    LastWriteTime = finfo.LastWriteTime,
                    Length = (finfo as FileInfo)?.Length ?? 0,
                };
            }
            else
            {

                //if (!finfo.Exists)
                //  continue;

                var fi = FindFileHelper(fileName.TrimStart('\\'));



                fileInfo = new FileInformation
                {
                    FileName = fileName,
                    Attributes = finfo.Attributes,
                    CreationTime = fi.CreationTime, // finfo.CreationTime,
                    LastAccessTime = fi.LastAccessTime, //finfo.LastAccessTime,
                    LastWriteTime = fi.LastWriteTime, // finfo.LastWriteTime,
                    Length = fi.Length // (finfo as FileInfo)?.Length ?? 0,
                };
                //    break;
                //}

            }
            return Trace(nameof(GetFileInformation), fileName, info, DokanResult.Success);
        }

        public NtStatus FindFiles(string fileName, out IList<FileInformation> files, IDokanFileInfo info)
        {
            // This function is not called because FindFilesWithPattern is implemented
            // Return DokanResult.NotImplemented in FindFilesWithPattern to make FindFiles called
            files = FindFilesHelper(fileName, "*");

            return Trace(nameof(FindFiles), fileName, info, DokanResult.Success);
        }

        public NtStatus SetFileAttributes(string fileName, FileAttributes attributes, IDokanFileInfo info)
        {
            try
            {
                // MS-FSCC 2.6 File Attributes : There is no file attribute with the value 0x00000000
                // because a value of 0x00000000 in the FileAttributes field means that the file attributes for this file MUST NOT be changed when setting basic information for the file
                if (attributes != 0)
                    File.SetAttributes(ResolveFileNamePath(fileName), attributes);
                return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.Success, attributes.ToString());
            }
            catch (UnauthorizedAccessException)
            {
                return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.AccessDenied, attributes.ToString());
            }
            catch (FileNotFoundException)
            {
                return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.FileNotFound, attributes.ToString());
            }
            catch (DirectoryNotFoundException)
            {
                return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.PathNotFound, attributes.ToString());
            }
        }

        public NtStatus SetFileTime(string fileName, DateTime? creationTime, DateTime? lastAccessTime,
            DateTime? lastWriteTime, IDokanFileInfo info)
        {
            try
            {
                if (info.Context is FileStream stream)
                {
                    var ct = creationTime?.ToFileTime() ?? 0;
                    var lat = lastAccessTime?.ToFileTime() ?? 0;
                    var lwt = lastWriteTime?.ToFileTime() ?? 0;
                    if (NativeMethods.SetFileTime(stream.SafeFileHandle, ref ct, ref lat, ref lwt))
                        return DokanResult.Success;
                    throw Marshal.GetExceptionForHR(Marshal.GetLastWin32Error());
                }

                var filePath = ResolveFileNamePath(fileName);

                if (creationTime.HasValue)
                    File.SetCreationTime(filePath, creationTime.Value);

                if (lastAccessTime.HasValue)
                    File.SetLastAccessTime(filePath, lastAccessTime.Value);

                if (lastWriteTime.HasValue)
                    File.SetLastWriteTime(filePath, lastWriteTime.Value);

                return Trace(nameof(SetFileTime), fileName, info, DokanResult.Success, creationTime, lastAccessTime,
                    lastWriteTime);
            }
            catch (UnauthorizedAccessException)
            {
                return Trace(nameof(SetFileTime), fileName, info, DokanResult.AccessDenied, creationTime, lastAccessTime,
                    lastWriteTime);
            }
            catch (FileNotFoundException)
            {
                return Trace(nameof(SetFileTime), fileName, info, DokanResult.FileNotFound, creationTime, lastAccessTime,
                    lastWriteTime);
            }
        }

        public NtStatus DeleteFile(string fileName, IDokanFileInfo info)
        {
            var filePath = ResolveFileNamePath(fileName);

            if (Directory.Exists(filePath))
                return Trace(nameof(DeleteFile), fileName, info, DokanResult.AccessDenied);

            if (!File.Exists(filePath))
                return Trace(nameof(DeleteFile), fileName, info, DokanResult.FileNotFound);

            if (File.GetAttributes(filePath).HasFlag(FileAttributes.Directory))
                return Trace(nameof(DeleteFile), fileName, info, DokanResult.AccessDenied);

            return Trace(nameof(DeleteFile), fileName, info, DokanResult.Success);
            // we just check here if we could delete the file - the true deletion is in Cleanup
        }

        public NtStatus DeleteDirectory(string fileName, IDokanFileInfo info)
        {
            throw new NotImplementedException();
            //return Trace(nameof(DeleteDirectory), fileName, info,
            //    Directory.EnumerateFileSystemEntries(GetPath(fileName)).Any()
            //        ? DokanResult.DirectoryNotEmpty
            //        : DokanResult.Success);
            //// if dir is not empty it can't be deleted
        }

        public NtStatus MoveFile(string oldName, string newName, bool replace, IDokanFileInfo info)
        {
            var oldpaths = GetPaths(oldName, false)[0];
            var newpaths = GetPaths(newName,false )[0];

            var oldpath = oldpaths.Item2;
            var newpath = newpaths.Item2; 

            (info.Context as FileStream)?.Dispose();
            info.Context = null;

            var exist = info.IsDirectory ? Directory.Exists(newpath) : File.Exists(newpath);

            try
            {

                if (!exist)
                {
                    info.Context = null;
                    if (info.IsDirectory)
                        Directory.Move(oldpath, newpath);
                    else
                        File.Move(oldpath, newpath);
                    return Trace(nameof(MoveFile), oldName, info, DokanResult.Success, newName,
                        replace.ToString(CultureInfo.InvariantCulture));
                }
                else if (replace)
                {
                    info.Context = null;

                    if (info.IsDirectory) //Cannot replace directory destination - See MOVEFILE_REPLACE_EXISTING
                        return Trace(nameof(MoveFile), oldName, info, DokanResult.AccessDenied, newName,
                            replace.ToString(CultureInfo.InvariantCulture));

                    File.Delete(newpath);
                    File.Move(oldpath, newpath);
                    return Trace(nameof(MoveFile), oldName, info, DokanResult.Success, newName,
                        replace.ToString(CultureInfo.InvariantCulture));
                }
            }
            catch (UnauthorizedAccessException)
            {
                return Trace(nameof(MoveFile), oldName, info, DokanResult.AccessDenied, newName,
                    replace.ToString(CultureInfo.InvariantCulture));
            }
            return Trace(nameof(MoveFile), oldName, info, DokanResult.FileExists, newName,
                replace.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus SetEndOfFile(string fileName, long length, IDokanFileInfo info)
        {
            //TODO: changed to allow splitting the as it becomes bigger.
            if (info.Context == null) return Trace(nameof(SetEndOfFile), fileName, info, DokanResult.Success,
                    length.ToString(CultureInfo.InvariantCulture));  
            try
            {
                ((FileStream)(info.Context)).SetLength(length);
                return Trace(nameof(SetEndOfFile), fileName, info, DokanResult.Success,
                    length.ToString(CultureInfo.InvariantCulture));
            }
            catch (IOException)
            {
                return Trace(nameof(SetEndOfFile), fileName, info, DokanResult.DiskFull,
                    length.ToString(CultureInfo.InvariantCulture));
            }
        }

        public NtStatus SetAllocationSize(string fileName, long length, IDokanFileInfo info)
        {
            try
            {
                ((FileStream)(info.Context)).SetLength(length);
                return Trace(nameof(SetAllocationSize), fileName, info, DokanResult.Success,
                    length.ToString(CultureInfo.InvariantCulture));
            }
            catch (IOException)
            {
                return Trace(nameof(SetAllocationSize), fileName, info, DokanResult.DiskFull,
                    length.ToString(CultureInfo.InvariantCulture));
            }
        }

        public NtStatus LockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {
#if !NETCOREAPP1_0
            try
            {
                ((FileStream)(info.Context)).Lock(offset, length);
                return Trace(nameof(LockFile), fileName, info, DokanResult.Success,
                    offset.ToString(CultureInfo.InvariantCulture), length.ToString(CultureInfo.InvariantCulture));
            }
            catch (IOException)
            {
                return Trace(nameof(LockFile), fileName, info, DokanResult.AccessDenied,
                    offset.ToString(CultureInfo.InvariantCulture), length.ToString(CultureInfo.InvariantCulture));
            }
#else
// .NET Core 1.0 do not have support for FileStream.Lock
            return DokanResult.NotImplemented;
#endif
        }

        public NtStatus UnlockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {
#if !NETCOREAPP1_0
            try
            {
                ((FileStream)(info.Context)).Unlock(offset, length);
                return Trace(nameof(UnlockFile), fileName, info, DokanResult.Success,
                    offset.ToString(CultureInfo.InvariantCulture), length.ToString(CultureInfo.InvariantCulture));
            }
            catch (IOException)
            {
                return Trace(nameof(UnlockFile), fileName, info, DokanResult.AccessDenied,
                    offset.ToString(CultureInfo.InvariantCulture), length.ToString(CultureInfo.InvariantCulture));
            }
#else
// .NET Core 1.0 do not have support for FileStream.Unlock
            return DokanResult.NotImplemented;
#endif
        }


    private string GetVolumeDriveFromVolumeId(string volumeId)
    {
        var ms = new ManagementObjectSearcher("Select * from Win32_Volume");
        foreach (var mo in ms.Get())
        {
            var guid = mo["DeviceID"].ToString();

            if (guid.Contains(volumeId))
                return (string)mo["DriveLetter"];
        }
        throw new Exception("Volume not found");
    }

    /// <summary>
    /// This deals with NFTS junctions
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    private string GetPathRoot(string path)
        {
            var p = path.TrimEnd('\\');
            var root = Path.GetPathRoot(path); 

            // it will test all the folders through the root, to see if any is a junction 
            while (p != root)
            {
                if (JunctionPoint.Exists(p))
                {
                    var target = JunctionPoint.GetTarget(p); // gets the real target
                    var vol = target.Substring("Volume{".Length).TrimEnd('\\').TrimEnd('}');
                    //var pathroot = Path.GetPathRoot(target); 
                    var drive = GetVolumeDriveFromVolumeId(vol);
                    return drive +"\\"; 

                    //"Volume{da5485fa-45cf-429e-906c-41904723c596}\\"
                }
                p = Directory.GetParent(p).FullName; // it will now check the parent. 
            }
            return root; // a junction was not found => return original root. 
        }

        private Dictionary<string, DriveInfo> GetDisksInfo(bool byDriveLetter = true)
        {
            var dinfoPerDrive = new Dictionary<string, DriveInfo>();
            var d = DriveInfo.GetDrives();

            foreach (var path in this.paths)
            {                
                var dinfo = d.Single(di => string.Equals(di.RootDirectory.Name, GetPathRoot(path), StringComparison.OrdinalIgnoreCase));
                dinfoPerDrive[byDriveLetter ? dinfo.Name : path] = dinfo;
            }
            return dinfoPerDrive; 
        }

        public NtStatus GetDiskFreeSpace(out long freeBytesAvailable, out long totalNumberOfBytes, out long totalNumberOfFreeBytes, IDokanFileInfo info)
        {
            freeBytesAvailable = 0; 
            totalNumberOfBytes = 0;  
            totalNumberOfFreeBytes = 0;

            var dinfoPerDrive = GetDisksInfo(); 

            foreach (var dinfo in dinfoPerDrive.Values)
            {
                freeBytesAvailable += dinfo.TotalFreeSpace;
                totalNumberOfBytes += dinfo.TotalSize;
                totalNumberOfFreeBytes += dinfo.AvailableFreeSpace;
            }
            return Trace(nameof(GetDiskFreeSpace), null, info, DokanResult.Success, "out " + freeBytesAvailable.ToString(),
                "out " + totalNumberOfBytes.ToString(), "out " + totalNumberOfFreeBytes.ToString());
        }

        public NtStatus GetVolumeInformation(out string volumeLabel, out FileSystemFeatures features,
            out string fileSystemName, out uint maximumComponentLength, IDokanFileInfo info)
        {
            volumeLabel = "Plot-DOKAN";
            fileSystemName = "NTFS";
            maximumComponentLength = 256;

            features = FileSystemFeatures.CasePreservedNames | FileSystemFeatures.CaseSensitiveSearch |
                       FileSystemFeatures.PersistentAcls | FileSystemFeatures.SupportsRemoteStorage |
                       FileSystemFeatures.UnicodeOnDisk;

            return Trace(nameof(GetVolumeInformation), null, info, DokanResult.Success, "out " + volumeLabel,
                "out " + features.ToString(), "out " + fileSystemName);
        }

        public NtStatus GetFileSecurity(string fileName, out FileSystemSecurity security, AccessControlSections sections,
            IDokanFileInfo info)
        {
            try
            {
#if NET5_0_OR_GREATER
                security = info.IsDirectory
                    ? (FileSystemSecurity)new DirectoryInfo(GetPath(fileName)).GetAccessControl()
                    : new FileInfo(GetPath(fileName)).GetAccessControl();
#else
                security = info.IsDirectory
                    ? (FileSystemSecurity)Directory.GetAccessControl(ResolveFileNamePath(fileName))
                    : File.GetAccessControl(ResolveFileNamePath(fileName));
#endif
                return Trace(nameof(GetFileSecurity), fileName, info, DokanResult.Success, sections.ToString());
            }
            catch (UnauthorizedAccessException)
            {
                security = null;
                return Trace(nameof(GetFileSecurity), fileName, info, DokanResult.AccessDenied, sections.ToString());
            }
        }

        public NtStatus SetFileSecurity(string fileName, FileSystemSecurity security, AccessControlSections sections,
            IDokanFileInfo info)
        {
            throw new NotImplementedException();
//            try
//            {
//#if NET5_0_OR_GREATER
//                if (info.IsDirectory)
//                {
//                    new DirectoryInfo(GetPath(fileName)).SetAccessControl((DirectorySecurity)security);
//                }
//                else
//                {
//                    new FileInfo(GetPath(fileName)).SetAccessControl((FileSecurity)security);
//                }
//#else
//                if (info.IsDirectory)
//                {
//                    Directory.SetAccessControl(GetPath(fileName), (DirectorySecurity)security);
//                }
//                else
//                {
//                    File.SetAccessControl(GetPath(fileName), (FileSecurity)security);
//                }
//#endif
//                return Trace(nameof(SetFileSecurity), fileName, info, DokanResult.Success, sections.ToString());
//            }
//            catch (UnauthorizedAccessException)
//            {
//                return Trace(nameof(SetFileSecurity), fileName, info, DokanResult.AccessDenied, sections.ToString());
//            }
        }

        public NtStatus Mounted(string mountPoint, IDokanFileInfo info)
        {
            return Trace(nameof(Mounted), null, info, DokanResult.Success);
        }

        public NtStatus Unmounted(IDokanFileInfo info)
        {
            return Trace(nameof(Unmounted), null, info, DokanResult.Success);
        }

        public NtStatus FindStreams(string fileName, IntPtr enumContext, out string streamName, out long streamSize,
            IDokanFileInfo info)
        {
            streamName = string.Empty;
            streamSize = 0;
            return Trace(nameof(FindStreams), fileName, info, DokanResult.NotImplemented, enumContext.ToString(),
                "out " + streamName, "out " + streamSize.ToString());
        }

        public NtStatus FindStreams(string fileName, out IList<FileInformation> streams, IDokanFileInfo info)
        {
            streams = new FileInformation[0];
            return Trace(nameof(FindStreams), fileName, info, DokanResult.NotImplemented);
        }

        public FileInformation FindFileHelper(string fileName)
        {
            //var filesMap = new Dictionary<string, FileInformation>();

            var m = new FileInformation();

            //foreach (var pathTuple in GetPaths(fileName, true))
            foreach(var file in GetReadFilePaths(fileName))
            {               

               // foreach (var file in files)
                {
               //     if (filesMap.ContainsKey(file.FileName))
                    {
                  //      var m = filesMap[file.FileName];
                        if (m.CreationTime > file.CreationTime) m.CreationTime = file.CreationTime;
                        if (m.LastAccessTime < file.LastAccessTime) m.LastAccessTime = file.LastAccessTime;
                        if (m.LastWriteTime < file.LastWriteTime) m.LastWriteTime = file.LastWriteTime;
                        m.Length += file.Length;
                        m.FileName = Path.GetFileName(file.FileName);
                        //filesMap[file.FileName] = m;
                    }
                    //else
                    //{
                     //   filesMap[file.FileName] = file;
                   // }
                }

                //    allFiles.AddRange(filesMap.Values); 
            }
            //return filesMap.Values.First();
            return m;
        }
        public IList<FileInformation> FindFilesHelper(string fileName, string searchPattern)
        {
            List<FileInformation> allFiles = new List<FileInformation>();
            var filesMap = new Dictionary<string, FileInformation>();
            foreach (var pathTuple in GetPaths(fileName,true))
            {
                var path = pathTuple.Item2;
                var basePath = pathTuple.Item1.Substring(this.basePath.Length).TrimEnd('\\');
                bool isRootofBase = pathTuple.Item2.TrimEnd('\\') == pathTuple.Item1.TrimEnd('\\'); 


                //IList<FileInformation>
                var files = new DirectoryInfo(path)
                    .EnumerateFileSystemInfos()
                    .Where(finfo => DokanHelper.DokanIsNameInExpression(searchPattern, finfo.Name, true))
                    .Select(finfo => new FileInformation
                    {
                        Attributes = finfo.Attributes,
                        CreationTime = finfo.CreationTime,
                        LastAccessTime = finfo.LastAccessTime,
                        LastWriteTime = finfo.LastWriteTime,
                        Length = (finfo as FileInfo)?.Length ?? 0,
                        FileName = //isRootofBase ? 
                                    //$"{basePath}{this.baseSeparator}{finfo.Name}" :
                                finfo.Name
                    });//.ToArray();

                foreach (var file in files)
                {
                    if (filesMap.ContainsKey(file.FileName))
                    {
                        var m = filesMap[file.FileName];
                        if (m.CreationTime > file.CreationTime) m.CreationTime = file.CreationTime;
                        if (m.LastAccessTime < file.LastAccessTime) m.LastAccessTime = file.LastAccessTime;
                        if (m.LastWriteTime < file.LastWriteTime) m.LastWriteTime = file.LastWriteTime;
                        m.Length += file.Length;
                        filesMap[file.FileName] = m;
                    }
                    else {
                        filesMap[file.FileName] = file; 
                    }
                }

            //    allFiles.AddRange(filesMap.Values); 
            }
            //return allFiles.ToArray();
            return filesMap.Values.ToArray(); 
        }

        public NtStatus FindFilesWithPattern(string fileName, string searchPattern, out IList<FileInformation> files,
            IDokanFileInfo info)
        {
            files = FindFilesHelper(fileName, searchPattern);

            return Trace(nameof(FindFilesWithPattern), fileName, info, DokanResult.Success);
        }

        #endregion Implementation of IDokanOperations
    }
}
