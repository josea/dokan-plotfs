using System;
using System.IO;
using DokanNet;

namespace DokanNetMirror
{
    internal class Notify : IDisposable
    {
        private readonly string[] _sourcePaths;
        private readonly string _targetPath;
        private readonly DokanInstance _dokanInstance;
        private readonly FileSystemWatcher[] _commonFsWatcher;
        private readonly FileSystemWatcher[] _fileFsWatcher;
        private readonly FileSystemWatcher[] _dirFsWatcher;
        private bool _disposed;

        public Notify(string[] mirrorPaths, string mountPath, DokanInstance dokanInstance)
        {
            _sourcePaths = mirrorPaths;
            _targetPath = mountPath;
            _dokanInstance = dokanInstance;
            _commonFsWatcher = new FileSystemWatcher[mirrorPaths.Length];
            _fileFsWatcher =  new FileSystemWatcher[mirrorPaths.Length];
            _dirFsWatcher = new FileSystemWatcher[mirrorPaths.Length];

            int i = 0;
            foreach (var mirrorPath in mirrorPaths)
            {
                _commonFsWatcher[i] = new FileSystemWatcher(mirrorPath)
                {
                    IncludeSubdirectories = true,
                    NotifyFilter = NotifyFilters.Attributes |
                        NotifyFilters.CreationTime |
                        NotifyFilters.DirectoryName |
                        NotifyFilters.FileName |
                        NotifyFilters.LastAccess |
                        NotifyFilters.LastWrite |
                        NotifyFilters.Security |
                        NotifyFilters.Size
                };

                _commonFsWatcher[i].Changed += OnCommonFileSystemWatcherChanged;
                _commonFsWatcher[i].Created += OnCommonFileSystemWatcherCreated;
                _commonFsWatcher[i].Renamed += OnCommonFileSystemWatcherRenamed;

                _commonFsWatcher[i].EnableRaisingEvents = true;

                _fileFsWatcher[i] = new FileSystemWatcher(mirrorPath)
                {
                    IncludeSubdirectories = true,
                    NotifyFilter = NotifyFilters.FileName
                };

                _fileFsWatcher[i].Deleted += OnCommonFileSystemWatcherFileDeleted;

                _fileFsWatcher[i].EnableRaisingEvents = true;

                _dirFsWatcher[i] = new FileSystemWatcher(mirrorPath)
                {
                    IncludeSubdirectories = true,
                    NotifyFilter = NotifyFilters.DirectoryName
                };

                _dirFsWatcher[i].Deleted += OnCommonFileSystemWatcherDirectoryDeleted;

                _dirFsWatcher[i].EnableRaisingEvents = true;
                i++;
            }

        }

        private string AlterPathToMountPath(string _sourcePath, string path)
        {
            var relativeMirrorPath = path.Substring(_sourcePath.Length).TrimStart('\\');

            return Path.Combine(_targetPath, relativeMirrorPath);
        }

        private void OnCommonFileSystemWatcherFileDeleted(object sender, FileSystemEventArgs e)
        {
            if (_dokanInstance.IsDisposed) return;

            var watcher = sender as FileSystemWatcher;
            var fullPath = AlterPathToMountPath(watcher.Path, e.FullPath);

            Dokan.Notify.Delete(_dokanInstance, fullPath, false);
        }

        private void OnCommonFileSystemWatcherDirectoryDeleted(object sender, FileSystemEventArgs e)
        {
            if (_dokanInstance.IsDisposed) return;

            var watcher = sender as FileSystemWatcher;
            var fullPath = AlterPathToMountPath(watcher.Path, e.FullPath);

            Dokan.Notify.Delete(_dokanInstance, fullPath, true);
        }

        private void OnCommonFileSystemWatcherChanged(object sender, FileSystemEventArgs e)
        {
            if (_dokanInstance.IsDisposed) return;
            
            var watcher = sender as FileSystemWatcher;
            var fullPath = AlterPathToMountPath(watcher.Path, e.FullPath);

            Dokan.Notify.Update(_dokanInstance, fullPath);
        }

        private void OnCommonFileSystemWatcherCreated(object sender, FileSystemEventArgs e)
        {
            if (_dokanInstance.IsDisposed) return;

            var watcher = sender as FileSystemWatcher;
            var fullPath = AlterPathToMountPath(watcher.Path, e.FullPath);

            var isDirectory = Directory.Exists(fullPath);

            Dokan.Notify.Create(_dokanInstance, fullPath, isDirectory);
        }

        private void OnCommonFileSystemWatcherRenamed(object sender, RenamedEventArgs e)
        {
            if (_dokanInstance.IsDisposed) return;
            var watcher = sender as FileSystemWatcher;
            
            var oldFullPath = AlterPathToMountPath(watcher.Path, e.OldFullPath);
            var oldDirectoryName = Path.GetDirectoryName(e.OldFullPath);

            var fullPath = AlterPathToMountPath(watcher.Path, e.FullPath);
            var directoryName = Path.GetDirectoryName(e.FullPath);

            var isDirectory = Directory.Exists(e.FullPath);
            var isInSameDirectory = String.Equals(oldDirectoryName, directoryName);

            Dokan.Notify.Rename(_dokanInstance, oldFullPath, fullPath, isDirectory, isInSameDirectory);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    for (var i = 0; i < _sourcePaths.Length; i++)
                    {
                        // dispose managed state (managed objects)
                        _commonFsWatcher[i].Changed -= OnCommonFileSystemWatcherChanged;
                        _commonFsWatcher[i].Created -= OnCommonFileSystemWatcherCreated;
                        _commonFsWatcher[i].Renamed -= OnCommonFileSystemWatcherRenamed;

                        _commonFsWatcher[i].Dispose();
                        _fileFsWatcher[i].Dispose();
                        _dirFsWatcher[i].Dispose();
                    }

                }

                // free unmanaged resources (unmanaged objects) and override finalizer
                // set large fields to null
                _disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
