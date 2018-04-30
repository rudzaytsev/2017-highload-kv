package ru.mail.polis.storage;

import org.jetbrains.annotations.NotNull;

import java.io.*;


/**
 * Created by rudolph on 04.02.18.
 */
public class StorageDao implements DAO {


  private final File dir;

  private static final Tombstone TOMBSTONE = new Tombstone();

  public StorageDao(File dir) {
    this.dir = dir;
  }


  @NotNull
  @Override
  public byte[] get(@NotNull String id) throws IdNotFoundException, DataDeletedException, IllegalArgumentException, IOException {
    File file = getFile(id);
    if (!file.exists())
      throw new IdNotFoundException("Can't get data by id = " + id);

    byte[] data = new byte[(int) file.length()];
    try (InputStream fis = new FileInputStream(file)){
      if (fis.read(data) != data.length)
        throw new IOException("Can't read file with name = " + id);
    }
    if (TOMBSTONE.isSetIn(data))
      throw new DataDeletedException("Value was deleted by id = " + id, TOMBSTONE.toString());

    return data;
  }

  private File getFile(String id) {
    return new File(dir, id);
  }

  @Override
  public void upsert(@NotNull String id, @NotNull byte[] data) throws IllegalArgumentException, IOException {
     File file = getFile(id);
     try (OutputStream fos = new FileOutputStream(file)) {
       fos.write(data);
     }
  }

  @Override
  public void delete(@NotNull String id) throws IllegalArgumentException, IOException {
    if (getFile(id).exists()) {
      upsert(id, TOMBSTONE.toBytes());
    }
  }
}
