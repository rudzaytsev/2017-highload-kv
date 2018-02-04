package ru.mail.polis.storage;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by rudolph on 04.02.18.
 */
public interface DAO {

  @NotNull byte[] get(@NotNull String id) throws NoSuchElementException, IllegalArgumentException, IOException;

  void upsert(@NotNull String id, @NotNull byte[] data) throws IllegalArgumentException, IOException;

  void delete(@NotNull String id) throws IllegalArgumentException, IOException;
}
