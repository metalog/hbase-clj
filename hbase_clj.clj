;; Copyright (c) 2009, Andrey Esipenko. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.


(ns hbase-clj
  (:use hbase-clj.constants clojure.contrib.def)
  (:import (org.apache.hadoop.hbase HConstants HBaseConfiguration 
				    HTableDescriptor HColumnDescriptor KeyValue)
	   (org.apache.hadoop.hbase.client HBaseAdmin HTable Put Get Scan Result)
	   (org.apache.hadoop.hbase.util Bytes)))


(def *hconf* (HBaseConfiguration.)) 	
;; (defonce *hconf* (HBaseConfiguration.))	; in production: no need to reload in slime

;; helpers
(definline k2s [str-or-keyw]
  `(if (keyword? ~str-or-keyw) (name ~str-or-keyw) ~str-or-keyw)) ; nil vs. ""?

(definline s2k [string]
  `(if (or (nil? ~string) (= "" ~string)) nil (keyword ~string))) ; or :nil ? that's the question!

(defn bytes 
  ([val]
     (cond
       (nil? val) +empty-byte-array+
       (string? val) (if (= 0 (count val))
		       +empty-byte-array+
		       (Bytes/toBytes val))
       (keyword? val) (Bytes/toBytes (name val))
       (= (class +empty-byte-array+) (class val)) val
       (or (seq? val) (vector? val) (set? val)) (apply bytes (seq val))
       :else (Bytes/toBytes val)))
  ([val & more]
     (reduce #(Bytes/add %1 (bytes %2)) (bytes val) more)))

(defalias b bytes)

(definline b2s
  [bytes]
  `(Bytes/toString ~bytes))

(definline b2k
  [bytes]
  `(s2k (b2s ~bytes)))

(definline b2sh
  [bytes]
  `(Bytes/toShort ~bytes))

(definline b2i
  [bytes]
  `(Bytes/toInt ~bytes))

(definline b2l
  [bytes]
  `(Bytes/toLong ~bytes))

(definline b2f
  [bytes]
  `(Bytes/toFloat ~bytes))

(definline b2d
  [bytes]
  `(Bytes/toDouble ~bytes))

(definline b2tf				;or b2b ?
  [bytes]
  `(Bytes/toBoolean ~bytes))

;; HAdmin
(definline hadmin []
  `(HBaseAdmin. *hconf*))		;

(definline list-tables []
  `(.listTables (hadmin)))

(declare htable-name)

(definline table-exists? [table]
  `(.tableExists (hadmin) (htable-name ~table)))

(definline get-table-descriptor [table]
  `(.getTableDescriptor (hadmin) (bytes (htable-name ~table))))

(definline delete-table [table]
  `(.deleteTable (hadmin) (htable-name ~table)))

(definline create-table [table]
  `(.createTable (hadmin) ~table))

;; Tables
(declare hcolumn-descriptor)
(defn htabdesc [table & families]
  (let [table (k2s table)
        tabdesc (HTableDescriptor. table)]
    (dorun (map #(.addFamily tabdesc (hcolumn-descriptor %)) families))
    tabdesc))
(defalias htd htabdesc)

(definline htable-name [table]
  `(if (or (keyword? ~table) (string? ~table)) 
     (k2s ~table)			; via given keystring
     (.getNameAsString ~table)))	; via given descriptor

(definline htable [table]
  `(HTable. *hconf* (htable-name ~table)))

(defmacro with-htable* 
  [tbl & body]
  (let [table (cond
		(string? tbl) (symbol tbl)
		(keyword? tbl) (symbol (name tbl))
		:else tbl)]
    `(with-open [~table (htable (str (quote ~table)))]
       ~@body)))

(defmacro with-htable
  [& args] 
  (let [tbls (butlast args)
	body (last args)
	first (first tbls)
	rest (next tbls)]
    (if rest
      `(with-htable* ~first (with-htable ~@rest ~body))
      `(with-htable* ~first ~body))))

(definline inc-column [table family column] ; experimental
  `(.incrementColumnValue (htable ~family) (b ~table) (b ~column) (b nil) 1))

(defn hcolumn-descriptor
  [desc]
  (cond
    (keyword? desc) (HColumnDescriptor. (name desc))
    (string? desc) (HColumnDescriptor. desc)
    (map? desc) (let [{:keys [family max-versions compression in-memory block-cache block-size ttl bloom]
			 :or {max-versions +default-versions+,
			      compression +default-compression+,
			      in-memory +default-in-memory+,
			      block-cache +default-blockcache+,
			      block-size +default-blocksize+,
			      ttl +default-ttl+,
			      bloom +default-bloomfilter+}} desc]
		    (HColumnDescriptor. (bytes family) max-versions compression in-memory block-cache block-size ttl bloom))))



;; Put
(defn- add-column 
  ([put family column value ts]
     (.add put (bytes family) (bytes column) ts (bytes value)))
  ([put family column value]
     (add-column put family column value +latest-timestamp+)))

(defn hput*
  [row & cols]
  (let [put (Put. (bytes row))] 
    (dorun (map #(apply add-column put %) cols))
    put))


(defn hput 
  ([table & rows]
     (if  (vector? (first rows))
       (.put table (map #(apply hput* %) rows))
       (.put table (apply hput* rows)))))

;; Get and Scan
(defn hget*
  [row {:keys [time-stamp time-range max-versions filter]
	:or {max-versions Integer/MAX_VALUE}} cols]
  (let [getobj (Get. (bytes row))]
    (.setFilter getobj filter)
    (.setMaxVersions getobj max-versions)
    (when time-stamp (.setTimeStamp getobj time-stamp))
    (when time-range (.setTimeRange getobj (first time-range) (last time-range)))
    (dorun (map #(if (vector? %)
		   (.addColumn getobj (bytes (first %)) (bytes (last %)))
		   (.addFamily getobj (bytes %))) cols))
    getobj))

(defn hget 
  [table row & cols-and-opts]
  (let [last-one (last cols-and-opts)
	opts (if (map? last-one) last-one {})
	cols (if (map? last-one) (butlast cols-and-opts) cols-and-opts)]
    (.get table (hget* row opts cols))))

(defn hscan*
  [rows {:keys [time-stamp time-range max-versions filter]
	 :or {max-versions Integer/MAX_VALUE}} cols]
  (let [scanobj (Scan. (-> rows first b) (-> rows second b))]
    (.setMaxVersions scanobj max-versions)
    (.setFilter scanobj filter)
    (when time-stamp (.setTimeStamp scanobj time-stamp))
    (when time-range (.setTimeRange scanobj (first time-range) (last time-range)))
    (dorun (map #(if (vector? %)
		   (.addColumn scanobj (bytes (first %)) (bytes (last %)))
		   (.addFamily scanobj (bytes %))) cols))
    scanobj))

(defn hscan
  ([table rows & cols-and-opts]
     (let [rows  (cond
		   (vector? rows) rows
		   (nil? rows) [+empty-start-row+ +empty-end-row+]
		   :else [rows +empty-end-row+])
	   last-one (last cols-and-opts)
	   opts (if (map? last-one) last-one {})
	   cols (if (map? last-one) (butlast cols-and-opts) cols-and-opts)]
       (.getScanner table (hscan* rows opts cols))))
  ([table]
     (hscan table nil)))


;; Result and KeyValue helpers.
;; TODO: rethink/refactor (.raw ..) from the user code to theese fns
;; e.g. (map-to-vec (hget ...)) (reduce-to-map (hget ...))
;; DONE-- in first approx
(defn to-vec
  "Used as a map fn to transform raw result kv-array to something more clojurish:
     (map to-vec (.raw (hget table rowid f1 [f2 q2] etc))"
  [kv]
  [(-> kv .getFamily b2k)
   (-> kv .getQualifier b2k)
   (-> kv .getTimestamp)
   (-> kv .getValue)])


(defn to-map
  "Used as a reduce fn to transform raw result kv-array to something more clojurish:
     (reduce to-map {} (.raw (hget table rowid f1 [f2 q2] etc))"
  [map kv]
  (assoc-in map
	    [(-> kv .getFamily b2k)
	     (-> kv .getQualifier b2k)
	     (-> kv .getTimestamp)]
	    (-> kv .getValue)))	   


(defn to-vmap
  [map kv]
  (update-in map
	    [(-> kv .getFamily b2k)
	     (-> kv .getQualifier b2k)]
	    #(apply vector %2 %1) [(-> kv .getTimestamp) (-> kv .getValue)]))

(defn to-nmap
  [map kv]
  (assoc-in map
	    [(-> kv .getFamily b2k)
	     (-> kv .getQualifier b2k)]
	    (-> kv .getValue)))

(definline hvector
  [kv]
  `(map to-vec (.raw ~kv)))
(defalias hvec hvector)

(definline hhmap
  [kv]
  `(reduce to-map {} (.raw ~kv)))
(defalias hmap hhmap)

(definline hvmap
  [kv]
  `(reduce to-vmap {} (.raw ~kv)))


(definline hnmap
  [kv]
  `(reduce to-nmap {} (.raw ~kv)))
(defalias hnvmap hnmap)