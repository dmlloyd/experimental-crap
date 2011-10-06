/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package com.flurg.crap.experimental.hashmaps;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Unlocked hash map.  Uses separate read and write views.  The write view is {@code null}ed while resize is taking
 * place.  After a write operation, if the write view has changed, the write is retried in case the copy to the resized
 * table missed the update.
 * <p>
 * Write operations are atomic but may appear in a different order than they occurred.  Or not.  Someone should
 * probably look into it.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class ConcurrentHashMapDML1<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V> {
    private static final int DEFAULT_INITIAL_CAPACITY = 512;
    private static final int MAXIMUM_CAPACITY = 1 << 30;
    private static final float DEFAULT_LOAD_FACTOR = 0.60f;

    private final Hasher<? super K> kh;
    private final Equaller<? super V> ve;

    @SuppressWarnings("unused")
    private volatile Table<K, V> readView;
    @SuppressWarnings("unused")
    private volatile Table<K, V> writeView;

    private final Set<Entry<K, V>> entrySet = new EntrySet();

    private final float loadFactor;
    private final int initialCapacity;

    @SuppressWarnings("unchecked")
    private static final AtomicIntegerFieldUpdater<Table> sizeUpdater = AtomicIntegerFieldUpdater.newUpdater(Table.class, "size");

    @SuppressWarnings("unchecked")
    private static final AtomicReferenceFieldUpdater<ConcurrentHashMapDML1, Table> readViewUpdater = AtomicReferenceFieldUpdater.newUpdater(ConcurrentHashMapDML1.class, Table.class, "readView");
    @SuppressWarnings("unchecked")
    private static final AtomicReferenceFieldUpdater<ConcurrentHashMapDML1, Table> writeViewUpdater = AtomicReferenceFieldUpdater.newUpdater(ConcurrentHashMapDML1.class, Table.class, "writeView");
    @SuppressWarnings("unchecked")
    private static final AtomicReferenceFieldUpdater<Item, Object> valueUpdater = AtomicReferenceFieldUpdater.newUpdater(Item.class, Object.class, "value");

    /**
     * Construct a new instance.
     *
     * @param keyHasher the key hasher
     * @param valueEqualler the value equaller
     * @param initialCapacity the initial capacity
     * @param loadFactor the load factor
     */
    public ConcurrentHashMapDML1(Hasher<? super K> keyHasher, Equaller<? super V> valueEqualler, int initialCapacity, float loadFactor) {
        if (keyHasher == null) {
            throw new IllegalArgumentException("keyHasher is null");
        }
        if (valueEqualler == null) {
            throw new IllegalArgumentException("valueEqualler is null");
        }
        kh = keyHasher;
        ve = valueEqualler;
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Initial capacity must be > 0");
        }
        if (initialCapacity > MAXIMUM_CAPACITY) {
            initialCapacity = MAXIMUM_CAPACITY;
        }
        if (loadFactor <= 0.0 || Float.isNaN(loadFactor) || loadFactor >= 1.0) {
            throw new IllegalArgumentException("Load factor must be between 0.0f and 1.0f");
        }

        int capacity = 1;

        while (capacity < initialCapacity) {
            capacity <<= 1;
        }

        this.loadFactor = loadFactor;
        this.initialCapacity = capacity;

        final Table<K, V> table = new Table<K, V>(capacity, loadFactor);
        readViewUpdater.set(this, table);
        writeViewUpdater.set(this, table);
    }

    /**
     * Construct a new instance.
     *
     * @param keyHasher the key hasher
     * @param valueEqualler the value equaller
     */
    public ConcurrentHashMapDML1(Hasher<? super K> keyHasher, Equaller<? super V> valueEqualler) {
        this(keyHasher, valueEqualler, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Construct a new instance.
     *
     * @param initialCapacity the initial capacity
     * @param loadFactor the load factor
     */
    public ConcurrentHashMapDML1(int initialCapacity, final float loadFactor) {
        this(Hasher.DEFAULT, Equaller.DEFAULT, initialCapacity, loadFactor);
    }

    /**
     * Construct a new instance.
     *
     * @param loadFactor the load factor
     */
    public ConcurrentHashMapDML1(final float loadFactor) {
        this(DEFAULT_INITIAL_CAPACITY, loadFactor);
    }

    /**
     * Construct a new instance.
     *
     * @param initialCapacity the initial capacity
     */
    public ConcurrentHashMapDML1(final int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Construct a new instance.
     */
    public ConcurrentHashMapDML1() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    private Table<K, V> getWriteView() {
        boolean intr = false;
        try {
            Table<K, V> view;
            view = writeView;
            if (view == null) {
                synchronized (this) {
                    view = writeView;
                    while (view == null) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            intr = true;
                        }
                        view = writeView;
                    }
                }
            }
            return view;
        } finally {
            if (intr) Thread.currentThread().interrupt();
        }
    }

    private Item<K, V> doGet(final AtomicReferenceArray<Item<K, V>[]> table, final K key) {
        boolean intr = false;
        try {
            Item<K, V>[] row = table.get(kh.hashCode(key) & (table.length() - 1));
            return row == null ? null : doGet(row, key);
        } finally {
            if (intr) Thread.currentThread().interrupt();
        }
    }

    private Item<K, V> doGet(Item<K, V>[] row, K key) {
        for (Item<K, V> item : row) {
            if (kh.equals(key, item.key)) {
                return item;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Item<K, V>[] addItem(final Item<K, V>[] row, final Item<K, V> newItem) {
        if (row == null) {
            return new Item[] { newItem };
        } else {
            final int length = row.length;
            Item<K, V>[] newRow = Arrays.copyOf(row, length + 1);
            newRow[length] = newItem;
            return newRow;
        }
    }

    private V doPut(K key, V value, boolean ifAbsent) {
        final int hashCode = kh.hashCode(key);

        OUTER: for (;;) {
            // Get our write view snapshot.
            Table<K, V> table = getWriteView();
            AtomicReferenceArray<Item<K, V>[]> array = table.array;
            int idx = hashCode & array.length() - 1;

            // Fetch the table row.
            Item<K, V>[] oldRow = array.get(idx);
            if (oldRow != null) {
                // Find the matching Item in the row.
                Item<K, V> oldItem = null;
                for (Item<K, V> tryItem : oldRow) {
                    if (kh.equals(key, tryItem.key)) {
                        oldItem = tryItem;
                        break;
                    }
                }
                if (oldItem != null) {
                    // entry exists; try to return the old value and try to replace the value if allowed.
                    V oldItemValue;
                    do {
                        oldItemValue = oldItem.value;
                        if (oldItemValue == DOORNAIL) {
                            // Key was removed; on the next iteration or two the doornail should be gone.
                            continue OUTER;
                        }
                    } while (! ifAbsent && ! valueUpdater.compareAndSet(oldItem, oldItemValue, value));
                    return oldItemValue;
                }
                // Row exists but item doesn't.
            }

            // Row doesn't exist, or row exists but item doesn't; try and add a new item to the row.
            final Item<K, V> newItem = new Item<K, V>(key, hashCode, value);
            final Item<K, V>[] newRow = addItem(oldRow, newItem);
            if (! array.compareAndSet(idx, oldRow, newRow)) {
                // Nope, row changed; retry.
                continue;
            }

            // Up the table size.
            if (sizeUpdater.getAndIncrement(table) == table.threshold) {
                resize(table);
            }

            // Success.
            return null;
        }
    }

    private static final Item<?,?>[] EMPTY_MARKER = new Item[1];

    @SuppressWarnings("unchecked")
    private void resize(Table<K, V> origTable) {
        if (! writeViewUpdater.compareAndSet(this, origTable, null)) {
            // someone else beat us to the punch
            return;
        }
        final AtomicReferenceArray<Item<K, V>[]> origArray = origTable.array;
        final int origCapacity = origArray.length();
        final Table<K, V> newTable = new Table<K, V>(origCapacity << 1, loadFactor);
        final AtomicReferenceArray<Item<K, V>[]> newArray = newTable.array;
        int newSize = 0;

        for (int i = 0; i < origCapacity; i ++) {
            // store marker replacement row with a null trailer so that in-progress write operations are always forced to retry
            Item<K, V>[] row, newRow;
            do {
                row = origArray.get(i);
                newRow = row == null ? (Item<K,V>[])EMPTY_MARKER : addItem(row, null);
            } while (! origArray.compareAndSet(i, row, newRow));
            if (row != null) for (Item<K, V> item : row) {
                final int idx = kh.hashCode(item.key) & (newArray.length() - 1);
                newArray.lazySet(idx, addItem(newArray.get(idx), item));
                newSize++;
            }
        }

        sizeUpdater.set(newTable, newSize);
        writeView = newTable;
        synchronized (this) {
            notifyAll();
        }
    }

    private static <K, V> Item<K, V>[] remove(Item<K, V>[] row, int idx) {
        final int len = row.length;
        assert idx < len;
        if (len == 1) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Item<K, V>[] newRow = new Item[len - 1];
        if (idx > 0) {
            System.arraycopy(row, 0, newRow, 0, idx);
        }
        if (idx < len - 1) {
            System.arraycopy(row, idx + 1, newRow, idx, len - 1 - idx);
        }
        return newRow;
    }

    public V putIfAbsent(final K key, final V value) {
        return doPut(key, value, true);
    }

    /**
     * Dead as a doornail.  Whoever sets this value on an Item is assured to have definitively removed the item from
     * the map.
     */
    private static final Object DOORNAIL = new Object();

    public boolean remove(final Object objectKey, final Object objectValue) {
        // Check key and value for validity.
        if (! (kh.accepts(objectKey) && ve.accepts(objectValue))) {
            return false;
        }

        // Get type-safe key and value.
        @SuppressWarnings("unchecked")
        final K key = (K) objectKey;
        @SuppressWarnings("unchecked")
        final V value = (V) objectValue;
        final int hashCode = kh.hashCode(key);

        // Get our first write view snapshot.
        Table<K, V> table = getWriteView();
        AtomicReferenceArray<Item<K, V>[]> array = table.array;
        int idx = hashCode & array.length() - 1;

        // Fetch the table row.
        Item<K, V>[] oldRow = array.get(idx);
        if (oldRow == null) {
            // no match for the key
            return false;
        }

        // Find the matching Item in the row.
        Item<K, V> oldItem = null;
        V oldValue = null;
        int rowIdx = -1;
        for (int i = 0; i < oldRow.length; i ++) {
            Item<K, V> tryItem = oldRow[i];
            if (kh.equals(key, tryItem.key)) {
                if (ve.equals(value, oldValue = tryItem.value)) {
                    oldItem = tryItem;
                    rowIdx = i;
                    break;
                } else {
                    // value doesn't match; exit without changing map.
                    return false;
                }
            }
        }
        if (oldItem == null) {
            // no such entry exists.
            return false;
        }

        // Mark the item as "removed".
        while (! valueUpdater.compareAndSet(oldItem, oldValue, DOORNAIL)) {
            if (ve.equals(value, oldValue = oldItem.value)) {
                // Values are equal; try marking it as removed again.
                continue;
            }
            // Value was changed to a non-equal value.
            return false;
        }

        // Now we are free to remove the item from the row.
        if (! array.compareAndSet(idx, oldRow, remove(oldRow, rowIdx))) {
            do {
                oldRow = array.get(idx);
                while (oldRow[oldRow.length - 1] == null) {
                    // table resize is in the offing!
                    // Get the new table and remove the row there.
                    table = getWriteView();
                    array = table.array;
                    idx = hashCode & array.length() - 1;
                    // Re-fetch the table row from the new table.
                    oldRow = array.get(idx);
                }
                rowIdx = kh.hashCode(key) & (array.length() - 1);
            } while (! array.compareAndSet(idx, oldRow, remove(oldRow, rowIdx)));
        }

        // Adjust the table size, since we are definitely the ones to be removing this item from the table.
        sizeUpdater.decrementAndGet(table);

        // Item is removed from the row; we are done here.
        return true;
    }

    public V remove(final Object objectKey) {
        // Check key for validity.
        if (! kh.accepts(objectKey)) {
            return null;
        }

        // Get type-safe key and value.
        @SuppressWarnings("unchecked")
        final K key = (K) objectKey;
        final int hashCode = kh.hashCode(key);

        // Get our first write view snapshot.
        Table<K, V> table = getWriteView();
        AtomicReferenceArray<Item<K, V>[]> array = table.array;
        int idx = hashCode & array.length() - 1;

        // Fetch the table row.
        Item<K, V>[] oldRow = array.get(idx);
        if (oldRow == null) {
            // no match for the key
            return null;
        }

        // Find the matching Item in the row.
        Item<K, V> oldItem = null;
        int rowIdx = -1;
        for (int i = 0; i < oldRow.length; i ++) {
            Item<K, V> tryItem = oldRow[i];
            if (kh.equals(key, tryItem.key)) {
                oldItem = tryItem;
                rowIdx = i;
                break;
            }
        }
        if (oldItem == null) {
            // no such entry exists.
            return null;
        }

        // Mark the item as "removed".
        @SuppressWarnings("unchecked")
        V oldValue = (V) valueUpdater.getAndSet(oldItem, DOORNAIL);
        if (oldValue == DOORNAIL) {
            // Someone else beat us to it.
            return null;
        }

        // Now we are free to remove the item from the row.
        if (! array.compareAndSet(idx, oldRow, remove(oldRow, rowIdx))) {
            do {
                oldRow = array.get(idx);
                while (oldRow[oldRow.length - 1] == null) {
                    // table resize is in the offing!
                    // Get the new table and remove the row there.
                    table = getWriteView();
                    array = table.array;
                    idx = hashCode & array.length() - 1;
                    // Re-fetch the table row from the new table.
                    oldRow = array.get(idx);
                }
                rowIdx = kh.hashCode(key) & (array.length() - 1);
            } while (! array.compareAndSet(idx, oldRow, remove(oldRow, rowIdx)));
        }

        // Adjust the table size, since we are definitely the ones to be removing this item from the table.
        sizeUpdater.decrementAndGet(table);

        // Item is removed from the row; we are done here.
        return oldValue;
    }

    public boolean replace(final K key, final V oldValue, final V newValue) {
        final int hashCode = kh.hashCode(key);

        // Get our write view snapshot.
        Table<K, V> table = getWriteView();
        AtomicReferenceArray<Item<K, V>[]> array = table.array;
        int idx = hashCode & array.length() - 1;

        // Fetch the table row.
        Item<K, V>[] oldRow = array.get(idx);
        if (oldRow == null) {
            // no match for the key
            return false;
        }

        // Find the matching Item in the row.
        Item<K, V> oldItem = null;
        V oldRowValue = null;
        for (Item<K, V> tryItem : oldRow) {
            if (kh.equals(key, tryItem.key)) {
                if (ve.equals(oldValue, oldRowValue = tryItem.value)) {
                    oldItem = tryItem;
                    break;
                } else {
                    // value doesn't match; exit without changing map.
                    return false;
                }
            }
        }
        if (oldItem == null) {
            // no such entry exists.
            return false;
        }

        // Now swap the item.
        while (! valueUpdater.compareAndSet(oldItem, oldRowValue, newValue)) {
            if (ve.equals(oldValue, oldRowValue = oldItem.value)) {
                // Values are equal; try swapping it removed again.
                continue;
            }
            // Value was changed to a non-equal value.
            return false;
        }

        // Item is swapped; we are done here.
        return true;
    }

    public V replace(final K key, final V value) {
        final int hashCode = kh.hashCode(key);

        // Get our write view snapshot.
        Table<K, V> table = getWriteView();
        AtomicReferenceArray<Item<K, V>[]> array = table.array;
        int idx = hashCode & array.length() - 1;

        // Fetch the table row.
        Item<K, V>[] oldRow = array.get(idx);
        if (oldRow == null) {
            // no match for the key
            return null;
        }

        // Find the matching Item in the row.
        Item<K, V> oldItem = null;
        for (Item<K, V> tryItem : oldRow) {
            if (kh.equals(key, tryItem.key)) {
                oldItem = tryItem;
                break;
            }
        }
        if (oldItem == null) {
            // no such entry exists.
            return null;
        }

        // Now swap the item.
        @SuppressWarnings("unchecked")
        V oldRowValue = (V) valueUpdater.getAndSet(oldItem, value);
        if (oldRowValue == DOORNAIL) {
            // Item was removed.
            return null;
        }

        // Item is swapped; we are done here.
        return oldRowValue;
    }

    public int size() {
        return readView.size;
    }

    public boolean containsKey(final Object key) {
        if (kh.accepts(key)) {
            @SuppressWarnings("unchecked")
            final Item<K, V> item = doGet(readView.array, (K) key);
            return item != null && item.value != DOORNAIL;
        } else {
            return false;
        }
    }

    public V get(final Object key) {
        if (kh.accepts(key)) {
            @SuppressWarnings("unchecked")
            final V value = doGet(readView.array, (K) key).value;
            return value == DOORNAIL ? null : value;
        } else {
            return null;
        }
    }

    public V put(final K key, final V value) {
        return doPut(key, value, false);
    }

    public void clear() {
        final Table<K, V> newTable = new Table<K, V>(initialCapacity, loadFactor);
        synchronized (this) {
            writeViewUpdater.set(this, null);
            readViewUpdater.set(this, newTable);
            writeViewUpdater.set(this, newTable);
            notifyAll();
        }
    }

    public Set<Entry<K, V>> entrySet() {
        return entrySet;
    }

    private final class EntrySet extends AbstractSet<Entry<K, V>> implements Set<Entry<K, V>> {

        public Iterator<Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        public int size() {
            return ConcurrentHashMapDML1.this.size();
        }
    }

    private final class EntryIterator implements Iterator<Entry<K, V>> {
        private final Table<K, V> table = readView;
        private int tableIdx;
        private int itemIdx;
        private Item<K, V> next;

        public boolean hasNext() {
            while (next == null) {
                final AtomicReferenceArray<Item<K, V>[]> array = table.array;
                if (array.length() == tableIdx) {
                    return false;
                }
                final Item<K, V>[] items = array.get(tableIdx);
                if (items != null) {
                    final int len = items.length;
                    if (itemIdx < len) {
                        if ((next = items[itemIdx++]) != null) {
                            return true;
                        }
                    }
                }
                itemIdx = 0;
                tableIdx++;
            }
            return true;
        }

        public Entry<K, V> next() {
            if (hasNext()) try {
                return next;
            } finally {
                next = null;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static final class Table<K, V> {
        final AtomicReferenceArray<Item<K, V>[]> array;
        final int threshold;
        volatile int size;

        private Table(int capacity, float loadFactor) {
            array = new AtomicReferenceArray<Item<K, V>[]>(capacity);
            threshold = capacity == MAXIMUM_CAPACITY ? Integer.MAX_VALUE : (int)(capacity * loadFactor);
        }
    }

    static final class Item<K, V> implements Entry<K, V> {
        private final K key;
        private final int hashCode;
        volatile V value;

        Item(final K key, final int hashCode, final V value) {
            this.key = key;
            this.hashCode = hashCode;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        public V setValue(final V value) {
            try {
                return this.value;
            } finally {
                this.value = value;
            }
        }

        public int hashCode() {
            return hashCode;
        }

        public boolean equals(final Object obj) {
            return obj instanceof Item && equals((Item<?,?>) obj);
        }

        public boolean equals(final Item<?, ?> obj) {
            return obj != null && hashCode == obj.hashCode && key.equals(obj.key);
        }
    }
}
