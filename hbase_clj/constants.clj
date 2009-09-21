(ns hbase-clj.constants
  (:use clojure.contrib.def)
  (:import (org.apache.hadoop.hbase HConstants HColumnDescriptor)
	   (org.apache.hadoop.hbase.util Bytes)))

;; HConstants

(defvar +empty-byte-array+       HConstants/EMPTY_BYTE_ARRAY)
(defvar +empty-start-row+        HConstants/EMPTY_START_ROW)
(defvar +empty-end-row+          HConstants/EMPTY_END_ROW)
(defvar +last-row+               HConstants/LAST_ROW)
(defvar +max-row-length+         HConstants/MAX_ROW_LENGTH)
(defvar +latest-timestamp+       HConstants/LATEST_TIMESTAMP)
(defvar +latest-timestamp-bytes+ HConstants/LATEST_TIMESTAMP_BYTES)
(defvar +all-versions+           HConstants/ALL_VERSIONS)
(defvar +forever+                HConstants/FOREVER)

(defvar +default-versions+       HColumnDescriptor/DEFAULT_VERSIONS)
(defvar +default-compression+    HColumnDescriptor/DEFAULT_COMPRESSION)
(defvar +default-in-memory+      HColumnDescriptor/DEFAULT_IN_MEMORY)
(defvar +default-blockcache+     HColumnDescriptor/DEFAULT_BLOCKCACHE)
(defvar +default-blocksize+      HColumnDescriptor/DEFAULT_BLOCKSIZE)
(defvar +default-ttl+            HColumnDescriptor/DEFAULT_TTL)
(defvar +default-bloomfilter+    HColumnDescriptor/DEFAULT_BLOOMFILTER)

(defvar +sizeof-boolean+         Bytes/SIZEOF_BOOLEAN)
(defvar +sizeof-byte+            Bytes/SIZEOF_BYTE)
(defvar +sizeof-char+            Bytes/SIZEOF_CHAR)
(defvar +sizeof-double+          Bytes/SIZEOF_DOUBLE)
(defvar +sizeof-float+           Bytes/SIZEOF_FLOAT)
(defvar +sizeof-int+             Bytes/SIZEOF_INT)
(defvar +sizeof-long+            Bytes/SIZEOF_LONG)
(defvar +sizeof-short+           Bytes/SIZEOF_SHORT)
