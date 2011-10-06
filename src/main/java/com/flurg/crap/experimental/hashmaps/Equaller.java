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
 * An equals-comparator.
 *
 * @param <T> the type to compare
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface Equaller<T> {

    /**
     * Determine whether the given object can be compared with this equaller.
     *
     * @param obj the object
     * @return {@code true} if it can be compared, {@code false} otherwise
     */
    boolean accepts(Object obj);

    /**
     * Test the two objects for equality.
     *
     * @param obj the object to compare
     * @param other the other object
     * @return {@code true} if they are equivalent, {@code false} otherwise
     */
    boolean equals(T obj, T other);

    Equaller<Object> IDENTITY = new IdentityHasher();

    Equaller<Object> DEFAULT = new DefaultHasher();

    Equaller<String> CASELESS = new CaselessEqualler();

    Equaller<byte[]> BYTE_ARRAY = new ByteArrayEqualler();

    Equaller<char[]> CHAR_ARRAY = new CharArrayEqualler();
}

class IdentityEqualler implements Equaller<Object>, Serializable {

    private static final long serialVersionUID = -749526530940615408L;

    public boolean accepts(final Object obj) {
        return true;
    }

    public boolean equals(final Object obj, final Object other) {
        return obj == other;
    }

    protected Object readResolve() {
        return Equaller.IDENTITY;
    }
}

class DefaultEqualler implements Equaller<Object>, Serializable {

    private static final long serialVersionUID = -5237758393814640207L;

    public boolean accepts(final Object obj) {
        return true;
    }

    public boolean equals(final Object obj, final Object other) {
        return obj == null ? other == null : obj.equals(other);
    }

    protected Object readResolve() {
        return Equaller.DEFAULT;
    }
}

class CaselessEqualler implements Equaller<String>, Serializable {

    private static final long serialVersionUID = -6008391944794806822L;

    public boolean accepts(final Object obj) {
        return obj instanceof String;
    }

    public boolean equals(final String obj, final String other) {
        return obj == null ? other == null : obj.equalsIgnoreCase(other);
    }

    protected Object readResolve() {
        return CASELESS;
    }
}

class ByteArrayEqualler implements Equaller<byte[]>, Serializable {

    private static final long serialVersionUID = -4553051228104567080L;

    public boolean accepts(final Object obj) {
        return obj instanceof byte[];
    }

    public boolean equals(final byte[] obj, final byte[] other) {
        return obj == null ? other == null : Arrays.equals(obj, other);
    }

    protected Object readResolve() {
        return Equaller.BYTE_ARRAY;
    }
}

class CharArrayEqualler implements Equaller<char[]>, Serializable {

    private static final long serialVersionUID = -8297351797791801773L;

    public boolean accepts(final Object obj) {
        return obj instanceof char[];
    }

    public boolean equals(final char[] obj, final char[] other) {
        return obj == null ? other == null : Arrays.equals(obj, other);
    }

    protected Object readResolve() {
        return Equaller.CHAR_ARRAY;
    }
}

