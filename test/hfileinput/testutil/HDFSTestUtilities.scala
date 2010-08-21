package hfileinput.testutil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils

object HDFSTestUtilities {

  def deleteHDFSFile(config: Configuration, path: Path): Unit = {
    val fs = path.getFileSystem(config)

    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  def duplicateFile(config: Configuration, source: Path, destination: Path): Unit = {
    val bufferSize = 100 * 1024 * 1024 // 100 MB
    val ifs = source.getFileSystem(config)
    val ofs = destination.getFileSystem(config)
    
    val is = ifs.open(source, bufferSize)
    val os = ofs.create(destination, false, bufferSize)
    
    IOUtils.copyBytes(is, os, bufferSize, true)
  }

  def deleteDirectory(config: Configuration, path: Path): Unit = {
    val fs = path.getFileSystem(config)

    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  def createDirectory(config: Configuration, path: Path): Boolean = {
    val fs = path.getFileSystem(config)
    fs.mkdirs(path)
  }

}
