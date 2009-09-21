(ns hbase-test
  (:use clojure.test hbase-clj hbase-clj.constants)
  ;(:import (org.apache.hadoop.hbase.util Bytes))
)

;; Helpers
(deftest keyword-string
  (are [x y] (= x y)
       (k2s :keyword) "keyword"
       (k2s "") ""
       (s2k (k2s :keyword)) :keyword)
  (are [x] (nil? x) 
       (k2s nil)
       (s2k (k2s ""))
       (s2k (k2s nil))))
  
(deftest byte-helpers
  (are [x y] (= x y)
       +empty-byte-array+ (b nil)
       (-> "transitive" b b2s) "transitive"
       (-> :keyword b b2k) :keyword
       (-> "keyword" b seq) (-> :keyword b seq)
       (-> "keyword" b seq) (-> :keyword b b seq)
       (seq (b :a nil :b)) (seq (b ["a" "" (b "b")]))
       (-> nil b b2k) nil
       (-> true b b2tf) true
       (-> false b b2tf) false
       (-> 1 short b b2sh) 1
       (-> 1 b b2i) 1
       (-> 1 long b b2l) 1
       (-> 0.5 float b b2f) 0.5
       (-> 0.5 double b b2d) 0.5))

(def test-table-d (htd :test-clj
		       :meta
		       {:family :info, :max-versions 1}
		       {:family :activities, :max-versions +all-versions+}
		       :content))

;; HBaseAdmin
(comment 
; Takes a long time. It's better to create table manually in repl before running tests 
; within slime it's as simple as C-x C-e
(defn setup
  []
  (if (some #{:test-clj} (map #(-> % .getName b2k) (list-tables))) ; (table-exists? :test-clj) ;)
    (do
      (.disableTable (hadmin) (htable-name :test-clj))
      (delete-table :test-clj)))
  (create-table test-table-d)
  (hput test-clj
	["10" [:info :name "ten"]
	      [:content :test "testing"]]
	["20" [:content :test-20 "testing"]]))
)

(deftest h-table
  (are [x y] (= x y)
       (.hasFamily test-table-d (b "activities")) true
       (htable-name test-table-d) (.getNameAsString test-table-d)
       (htable-name test-table-d) (k2s :test-clj)
       (with-htable test-clj
	 (-> test-clj .getTableName b2k)) :test-clj))

;; TODO: hscan* has some edge cases with start and stop row, need to rethink


(deftest put-get-scan
  (with-htable test-clj
    (do
      (are [x y] (= (-> (map to-vec (.raw x)) first last b2s) y) ;hget
	   (hget test-clj "00" [:info :name]) nil
	   (hget test-clj "10" [:info :name]) "ten"
	   (hget test-clj "10" [:info :info]) nil
	   (hget test-clj "10" ["content" "test"]) "testing"
	   (hget test-clj "20" [:content "test-20"]) "testing")

      (with-open [rows (hscan test-clj ["10" "200"] :info :content)] ;hscan
	(let [result (map #(reduce to-map {} (.raw %)) rows)]
	  (are [x y] (= (b2s x) (b2s y) )
	       (-> (first result) :content :test vals first) (-> (second result) :content :test-20 vals first)
	       (-> (first result) :info :name vals first) (b "ten")
	       (-> (second result) :info :name vals first) nil))))))


(deftest results
  (with-htable test-clj
    (let [res   (hget test-clj "10")
	  vres  (hvec res)
	  mres  (hhmap res)
	  mvres (hvmap res)
	  mnres (hnmap res)]
      (are [x y] (=  x y)
	   (first (first vres)) :content
	   (second (first vres)) :test
	   (b2k (last (first vres))) :testing

	   (first (last vres)) :info
	   (second (last vres)) :name
	   (b2k (last (last vres))) :ten

	   (-> mres :info :name vals first b2k) :ten
	   (-> mres :content :test vals first b2k) :testing
	   
	   (-> mvres :info :name first second b2k) :ten
	   (-> mvres :content :test first second b2k) :testing

	   (-> mnres :info :name b2k) :ten
	   (-> mnres :content :test b2k) :testing
      ))))
(run-tests)
