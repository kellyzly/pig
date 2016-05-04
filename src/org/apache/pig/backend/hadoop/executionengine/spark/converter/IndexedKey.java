/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * IndexedKey records the index and key info.
 * This is used as key for JOINs. It addresses the case where key is
 * either empty (or is a tuple with one or more empty fields). In this case,
 * we must respect the SQL standard as documented in the equals() method.
 */
public class IndexedKey implements Serializable, Comparable {
    private static final Log LOG = LogFactory.getLog(IndexedKey.class);
    private byte index;
    private Object key;
    private boolean useSecondaryKey;
    private boolean[] secondarySortOrder;

    public byte getIndex() {
        return index;
    }

    public IndexedKey(byte index, Object key) {
        this.index = index;
        this.key = key;
    }

    public Object getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "IndexedKey{" +
                "index=" + index +
                ", key=" + key +
                '}';
    }

    /**
     * If key is empty, we'd like compute equality based on key and index.
     * If key is not empty, we'd like to compute equality based on just the key (like we normally do).
     * There are two possible cases when two tuples are compared:
     * 1) Compare tuples of same table (same index)
     * 2) Compare tuples of different tables (different index values)
     * In 1)
     * key1    key2    equal?
     * null    null      Y
     * foo     null      N
     * null    foo       N
     * foo     foo       Y
     * (1,1)   (1,1)     Y
     * (1,)    (1,)      Y
     * (1,2)   (1,2)     Y
     * <p/>
     * <p/>
     * In 2)
     * key1    key2    equal?
     * null    null     N
     * foo     null     N
     * null    foo      N
     * foo     foo      Y
     * (1,1)   (1,1)    Y
     * (1,)    (1,)     N
     * (1,2)   (1,2)    Y
     *
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexedKey that = (IndexedKey) o;
        if (index == that.index) {
            if (key == null && that.key == null) {
                return true;
            } else if (key == null || that.key == null) {
                return false;
            } else {
                if (useSecondaryKey) {
                    try {
                        Object firstKey = ((Tuple) key).get(0);
                        Object firstKeyOfThat = ((Tuple) that.key).get(0);
                        if (firstKey == null && firstKeyOfThat == null) {
                            return true;
                        } else if (firstKey == null || firstKeyOfThat == null) {
                            return false;
                        } else {
                            return firstKey.equals(firstKeyOfThat);
                        }
                    } catch (ExecException e) {
                        throw new RuntimeException("IndexedKey.equals# throw exception: ", e);
                    }
                } else {
                    return key.equals(that.key);
                }
            }
        } else {
            if (key == null || that.key == null) {
                return false;
            } else if (key.equals(that.key) && !containNullfields(key)) {
                return true;
            } else {
                return false;
            }
        }
    }

    private boolean containNullfields(Object key) {
        if (key instanceof Tuple) {
            for (int i = 0; i < ((Tuple) key).size(); i++) {
                try {
                    if (((Tuple) key).get(i) == null) {
                        return true;
                    }
                } catch (ExecException e) {
                    throw new RuntimeException("exception found in " +
                            "containNullfields", e);

                }
            }
        }
        return false;

    }

    /**
     * Calculate hashCode by index and key
     * if key is empty, return index value
     * if key is not empty, return the key.hashCode()
     */
    @Override
    public int hashCode() {
        int result = 0;
        if (key == null) {
            result = (int) index;
        } else {
            result = key.hashCode();
        }
        return result;
    }

    //firstly compare the index
    //secondly compare the key
    @Override
    public int compareTo(Object o) {
        IndexedKey that = (IndexedKey) o;
        int res = index - that.getIndex();
        if (res > 0) {
            return 1;
        } else if (res < 0) {
            return -1;
        } else {
            if( useSecondaryKey){
                Tuple thisCompoundKey = (Tuple) key;
                Tuple thatCompoundKey = (Tuple)that.getKey();
                try {
                    Object thisFirstKey =  thisCompoundKey.get(0);
                    Object thisSecondary= thisCompoundKey.get(1);
                    Object thatFirstKey = thatCompoundKey.get(0);
                    Object thatSecondaryKey = thatCompoundKey.get(1);
                    res = DataType.compare(thisFirstKey, thatFirstKey);
                    if( res == 0){
                        return compareSecondaryKeys(thisSecondary, thatSecondaryKey,  secondarySortOrder);
                    } else{
                        return res;
                    }
                } catch (ExecException e) {
                   throw new RuntimeException("IndexedKey#compareTo throws exception ",e);
                }
            } else {
                return DataType.compare(key, that.getKey());
            }
        }
    }

    public void setUseSecondaryKey(boolean useSecondaryKey) {
        this.useSecondaryKey = useSecondaryKey;
    }

    public void setSecondarySortOrder(boolean[] secondarySortOrder) {
        this.secondarySortOrder = secondarySortOrder;
    }


    private int compareSecondaryKeys(Object o1, Object o2, boolean[] asc) {
        int rc = 0;
        if (o1 != null && o2 != null && o1 instanceof Tuple && o2 instanceof Tuple) {
            // objects are Tuples, we may need to apply sort order inside them
            Tuple t1 = (Tuple) o1;
            Tuple t2 = (Tuple) o2;
            int sz1 = t1.size();
            int sz2 = t2.size();
            if (sz2 < sz1) {
                return 1;
            } else if (sz2 > sz1) {
                return -1;
            } else {
                for (int i = 0; i < sz1; i++) {
                    try {
                        rc = DataType.compare(t1.get(i), t2.get(i));
                        if (rc != 0 && asc != null && asc.length > 1 && !asc[i])
                            rc *= -1;
                        if ((t1.get(i) == null) || (t2.get(i) == null)) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("t1.get(i) is:" + t1.get(i) + " t2.get(i) is:" + t2.get(i));
                            }
                        }
                        if (rc != 0) break;
                    } catch (ExecException e) {
                        throw new RuntimeException("Unable to compare tuples", e);
                    }
                }
            }
        } else {
            // objects are NOT Tuples, delegate to DataType.compare()
            rc = DataType.compare(o1, o2);
        }
        // apply sort order for keys that are not tuples or for whole tuples
        if (asc != null && asc.length == 1 && !asc[0])
            rc *= -1;
        return rc;
    }
}