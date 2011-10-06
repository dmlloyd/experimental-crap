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

import java.io.Serializable;
import java.util.Arrays;

/**
 * @param <T> the upper bound of the type type of the key
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface Hasher<T> extends Equaller<T> {

    /**
     * Return the hash code of the given object.
     *
     * @param obj the object
     * @return the object's hash code
     */
    int hashCode(T obj);

    /**
     * A hasher which uses the standard {@code equals/hashCode} hashing mechanism.
     */
    Hasher<Object> DEFAULT = new DefaultHasher();

    /**
     * A hasher which uses the standard {@code equals/hashCode} hashing mechanism but spreads the hash code for under-performing {@code hashCode} implementations.
     */
    Hasher<Object> SPREADING = new SpreadingHasher();

    /**
     * A hasher which hashes based upon object identity.
     */
    Hasher<Object> IDENTITY = new IdentityHasher();

    /**
     * A hasher for {@code byte[]} keys.
     */
    Hasher<byte[]> BYTE_ARRAY = new ByteArrayHasher();

    /**
     * A hasher for {@code char[]} keys.
     */
    Hasher<char[]> CHAR_ARRAY = new CharArrayHasher();
}

final class DefaultHasher extends DefaultEqualler implements Hasher<Object>, Serializable {

    private static final long serialVersionUID = 1382908942098071506L;

    public int hashCode(final Object obj) {
        return obj.hashCode();
    }

    protected Object readResolve() {
        return Hasher.DEFAULT;
    }
}

final class SpreadingHasher extends DefaultEqualler implements Hasher<Object>, Serializable {

    private static final long serialVersionUID = 691033263957071459L;

    public int hashCode(final Object obj) {
        int h = obj.hashCode();
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    protected Object readResolve() {
        return Hasher.SPREADING;
    }
}

final class IdentityHasher extends IdentityEqualler implements Hasher<Object>, Serializable {

    private static final long serialVersionUID = -8738260175140199299L;

    public int hashCode(final Object obj) {
        return System.identityHashCode(obj);
    }

    protected Object readResolve() {
        return Hasher.IDENTITY;
    }
}

class ByteArrayHasher extends ByteArrayEqualler implements Hasher<byte[]>, Serializable {

    private static final long serialVersionUID = 5696770288979450979L;

    public int hashCode(final byte[] obj) {
        return Arrays.hashCode(obj);
    }

    protected Object readResolve() {
        return Hasher.BYTE_ARRAY;
    }
}

class CharArrayHasher extends CharArrayEqualler implements Hasher<char[]>, Serializable {

    private static final long serialVersionUID = 5365540129450786824L;

    public int hashCode(final char[] obj) {
        return Arrays.hashCode(obj);
    }

    protected Object readResolve() {
        return Hasher.CHAR_ARRAY;
    }
}
