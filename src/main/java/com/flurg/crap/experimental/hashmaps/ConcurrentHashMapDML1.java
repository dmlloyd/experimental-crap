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
import java.util.Collection;
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

    /**
     * Dead as a doornail.  Whoever sets this value on an Item is assured to have definitively removed the item from
     * the map.
     */
    private static final Object DOORNAIL = new Object();

    /**
     * Differentiate between a {@code null} value and a missing value for put-if-absent operations.
     */
    private static final Object NOT_PRESENT = new Object();

    @SuppressWarnings("unused")
    private volatile Table<K, V> readView;
    @SuppressWarnings("unused")
    private volatile Table<K, V> writeView;

    private final Set<Entry<K, V>> entrySet = new EntrySet();
    private final Set<V> values = new ValueSet();

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
     * @param initialCapacity the initial capacity
     * @param loadFactor the load factor
     */
    public ConcurrentHashMapDML1(int initialCapacity, float loadFactor) {
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

    private Item<K, V> doGet(final AtomicReferenceArray<Item<K, V>[]> table, final Object key) {
        boolean intr = false;
        try {
            Item<K, V>[] row = table.get(key.hashCode() & (table.length() - 1));
            return row == null ? null : doGet(row, key);
        } finally {
            if (intr) Thread.currentThread().interrupt();
        }
    }

    private Item<K, V> doGet(Item<K, V>[] row, Object key) {
        for (Item<K, V> item : row) {
            if (key.equals(item.key)) {
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

    @SuppressWarnings("unchecked")
    private V doPut(K key, V value, boolean ifAbsent) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        final int hashCode = key.hashCode();

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
                    if (key.equals(tryItem.key)) {
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
            return (V) NOT_PRESENT;
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
                final int idx = item.key.hashCode() & (newArray.length() - 1);
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
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        V result = doPut(key, value, true);
        return result == NOT_PRESENT ? null : result;
    }

    public boolean remove(final Object key, final Object value) {
        if (key == null) return false;

        final int hashCode = key.hashCode();

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
            if (key.equals(tryItem.key)) {
                final Object other = oldValue = tryItem.value;
                if (equals(value, other)) {
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
            oldValue = oldItem.value;
            if (equals(value, oldValue)) {
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
                rowIdx = key.hashCode() & (array.length() - 1);
            } while (! array.compareAndSet(idx, oldRow, remove(oldRow, rowIdx)));
        }

        // Adjust the table size, since we are definitely the ones to be removing this item from the table.
        sizeUpdater.decrementAndGet(table);

        // Item is removed from the row; we are done here.
        return true;
    }

    public V remove(final Object key) {
        if (key == null) return null;

        final int hashCode = key.hashCode();

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
            if (key.equals(tryItem.key)) {
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
                rowIdx = hashCode & (array.length() - 1);
            } while (! array.compareAndSet(idx, oldRow, remove(oldRow, rowIdx)));
        }

        // Adjust the table size, since we are definitely the ones to be removing this item from the table.
        sizeUpdater.decrementAndGet(table);

        // Item is removed from the row; we are done here.
        return oldValue;
    }

    private void removeItem(final Item<K, V> item) {
        final int hashCode = item.hashCode;

        // Get our first write view snapshot.
        Table<K, V> table = getWriteView();
        AtomicReferenceArray<Item<K, V>[]> array = table.array;
        int idx = hashCode & array.length() - 1;

        // Fetch the table row.
        Item<K, V>[] oldRow = array.get(idx);
        if (oldRow == null) {
            // no match for the key
            throw new IllegalStateException("Item already removed");
        }

        // Find the matching Item in the row.
        Item<K, V> oldItem = null;
        int rowIdx = -1;
        for (int i = 0; i < oldRow.length; i ++) {
            Item<K, V> tryItem = oldRow[i];
            if (item == tryItem) {
                oldItem = tryItem;
                rowIdx = i;
                break;
            }
        }
        if (oldItem == null) {
            // no such entry exists.
            throw new IllegalStateException("Item already removed");
        }

        // Mark the item as "removed".
        @SuppressWarnings("unchecked")
        V oldValue = (V) valueUpdater.getAndSet(oldItem, DOORNAIL);
        if (oldValue == DOORNAIL) {
            // Someone else beat us to it.
            throw new IllegalStateException("Item already removed");
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
                rowIdx = hashCode & (array.length() - 1);
            } while (! array.compareAndSet(idx, oldRow, remove(oldRow, rowIdx)));
        }

        // Adjust the table size, since we are definitely the ones to be removing this item from the table.
        sizeUpdater.decrementAndGet(table);

        // Item is removed from the row; we are done here.
    }

    public boolean replace(final K key, final V oldValue, final V newValue) {
        if (key == null) return false;

        final int hashCode = key.hashCode();

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
            if (key.equals(tryItem.key)) {
                final Object other = oldRowValue = tryItem.value;
                if (equals(oldValue, other)) {
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
            final Object other = oldRowValue = oldItem.value;
            if (equals(oldValue,  other)) {
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
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        final int hashCode = key.hashCode();

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
            if (key.equals(tryItem.key)) {
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
        final Item<K, V> item = doGet(readView.array, key);
        return item != null && item.value != DOORNAIL;
    }

    public V get(final Object key) {
        final V value = doGet(readView.array, key).value;
        return value == DOORNAIL ? null : value;
    }

    public V put(final K key, final V value) {
        V result = doPut(key, value, false);
        return result == NOT_PRESENT ? null : result;
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

    public Collection<V> values() {
        return values;
    }

    final class ValueSet extends AbstractSet<V> implements Set<V> {

        public Iterator<V> iterator() {
            return new ValueIterator();
        }

        public int size() {
            return ConcurrentHashMapDML1.this.size();
        }

        public void clear() {
            ConcurrentHashMapDML1.this.clear();
        }
    }

    final class EntrySet extends AbstractSet<Entry<K, V>> implements Set<Entry<K, V>> {

        public Iterator<Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        public boolean remove(final Object o) {
            return o instanceof Entry && ConcurrentHashMapDML1.this.remove(((Entry<?, ?>) o).getKey(), ((Entry<?, ?>) o).getValue());
        }

        public boolean add(final Entry<K, V> entry) {
            return doPut(entry.getKey(), entry.getValue(), true) == NOT_PRESENT;
        }

        public void clear() {
            ConcurrentHashMapDML1.this.clear();
        }

        public boolean contains(final Object o) {
            if (o instanceof Item) {
                final Item<?, ?> otherItem = (Item<?, ?>) o;
                final Item<K, V> item = doGet(readView.array, otherItem.getKey());
                Object otherVal = otherItem.getValue();
                V itemVal = item.value;
                return item != null && itemVal != DOORNAIL && otherVal != DOORNAIL && otherItem.key.equals(item.key) && ConcurrentHashMapDML1.equals(itemVal, otherVal);
            }
            return false;
        }

        public int size() {
            return ConcurrentHashMapDML1.this.size();
        }
    }

    static boolean equals(final Object left, final Object right) {
        return left == null ? right == null : left.equals(right);
    }

    final class ValueIterator implements Iterator<V> {
        private final Table<K, V> table = readView;
        private int tableIdx;
        private int itemIdx;
        private Item<K, V>[] row;
        private Item<K, V> next;
        private Item<K, V> prev;
        private V nextVal;

        public boolean hasNext() {
            while (next == null) {
                final AtomicReferenceArray<Item<K, V>[]> array = table.array;
                Item<K, V>[] items = row;
                while (items == null) {
                    if (array.length() == tableIdx) {
                        next = null;
                        nextVal = null;
                        return false;
                    }
                    row = items = array.get(tableIdx++);
                }
                final int len = items.length;
                if (itemIdx < len) {
                    if ((next = items[itemIdx++]) != null && (nextVal = next.value) != DOORNAIL) {
                        return true;
                    }
                }
                itemIdx = 0;
                tableIdx++;
            }
            return true;
        }

        public V next() {
            if (hasNext()) try {
                return nextVal;
            } finally {
                prev = next;
                nextVal = null;
                next = null;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            if (prev == null) {
                throw new NoSuchElementException();
            } else try {
                removeItem(prev);
            } finally {
                prev = null;
            }
        }

    }

    final class EntryIterator implements Iterator<Entry<K, V>> {
        private final Table<K, V> table = readView;
        private int tableIdx;
        private int itemIdx;
        private Item<K, V>[] row;
        private Item<K, V> next;
        private Item<K, V> prev;

        public boolean hasNext() {
            while (next == null) {
                final AtomicReferenceArray<Item<K, V>[]> array = table.array;
                Item<K, V>[] items = row;
                while (items == null) {
                    if (array.length() == tableIdx) {
                        return false;
                    }
                    row = items = array.get(tableIdx++);
                }
                final int len = items.length;
                if (itemIdx < len) {
                    if ((next = items[itemIdx++]) != null) {
                        return true;
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
                prev = next;
                next = null;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            if (prev == null) {
                throw new NoSuchElementException();
            } else try {
                removeItem(prev);
            } finally {
                prev = null;
            }
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
            V oldValue;
            do {
                oldValue = this.value;
                if (oldValue == DOORNAIL) {
                    throw new IllegalStateException();
                }
            } while (! valueUpdater.compareAndSet(this, oldValue, value));
            return oldValue;
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
