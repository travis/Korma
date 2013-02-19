(ns korma.seql
  "A new syntax for writing SQL in Korma"
  (:require [clojure.string :as str]
            [korma.sql.engine :as eng]))

(defrecord Entity [name])

(defprotocol ToEntity
  (to-entity [obj]))

(extend-protocol ToEntity
  Entity
  (to-entity [entity] entity)
  String
  (to-entity [name] (->Entity name {}))
  clojure.lang.Keyword
  (to-entity [kw] (->Entity (name kw) {})))

(defmacro defentity [name & body])

(deftype Query [components]
  clojure.lang.Seqable
  (seq [this] (prn components)))

(defprotocol Clause
  (add-to-query [clause query]))

(deftype AndPredicate [pred1 pred2])

(deftype OrPredicate [pred1 pred2])

(deftype SimplePredicate [operator x y])

(deftype IsPredicate [field])

(deftype IsNotPredicate [field])

(deftype InPredicate [field list])

(deftype BetweenPredicate [field min max])

(deftype LikePredicate [field like])

(deftype FieldsClause [fields]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)]
      (if-let [clause (:fields components)]
        (->Query (assoc components :fields (FieldsClause. (concat fields (.fields clause)))))
        (->Query (assoc components :fields clause))))))

(deftype WhereClause [predicate]
  Clause
  (add-to-query [where query]
    (let [components (.components query)]
      (if-let [existing-where (:where components)]
        (->Query (assoc components :where (WhereClause. (->AndPredicate (.predicate existing-where)
                                                                        (.predicate where)))))
        (->Query (assoc components :where where))))))

(deftype OrderClause [fields direction]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)]
      (if-let [clause (:order components)]
        (->Query (assoc components :order (OrderClause. (concat fields (.fields clause)) (.direction clause))))
        (->Query (assoc components :order clause))))))

(deftype LimitClause [limit]
  Clause
  (add-to-query [clause query]
    (->Query (assoc (.components query) :limit clause))))

(deftype OffsetClause [offset]
  Clause
  (add-to-query [clause query]
    (->Query (assoc (.components query) :offset clause))))

;;; dsl

(defn select [entity & clauses]
  (reduce (fn [query clause] (add-to-query clause query))
          (->Query {:type :select :entity (to-entity entity)}) clauses))

(defn fields [& fields]
  (->FieldsClause fields))

(defn where
  ([predicate] (->WhereClause predicate))
  ([key val] (where (SimplePredicate. "=" key val))))

(defn order
  ([direction & fields] (->OrderClause fields direction))
  ([field] (order :asc field)))

(defn limit [limit]
  (->LimitClause limit))

(defn offset [offset]
  (->OffsetClause offset))

(defprotocol Sqlable
  (as-sql [sqlable]))

(def ^:dynamic *delimiters* ["\"" "\""])
(def ^:dynamic *table* nil)

(defn delimit [value]
  (let [[open close] *delimiters*]
    (str open value close)))

(defn qualify-field
  ([table field] (str (delimit table)"."(delimit field)))
  ([field] (if *table*
             (qualify-field (name *table*) field)
             (delimit field))))

(defn sanitize-field [field]
  (apply qualify-field (str/split (name field) #"\.")))

(defn to-order [order]
  (if (#{:desc "desc" :DESC "DESC"} order) "DESC" "ASC"))

(defn clauses-to-sql [components clause-names]
  (str/join " "
            (remove nil?
                    (map #(if-let [component (components %)] (as-sql component) nil)
                         clause-names))))

(extend-protocol Sqlable
  Entity
  (as-sql [entity]
    (delimit (.name entity)))
  Query
  (as-sql [query]
    (let [components (.components query)
          entity (:entity components)
          fields (:fields components)]
      (prn components)
      (binding [*table* (.name entity)]
        (str "SELECT "
             (if fields (as-sql fields) "*")
             " FROM "
             (clauses-to-sql
              components
              [:entity #_:joins :where #_:group #_:having :order
               :limit :offset])))))
  FieldsClause
  (as-sql [clause]
    (str (str/join ", " (map as-sql (.fields clause)))))
  WhereClause
  (as-sql [clause]
    (str "WHERE ("(as-sql (.predicate clause))")"))
  OrderClause
  (as-sql [clause]
    (str "ORDER BY "(str/join ", " (map as-sql (.fields clause)))" "(to-order (.direction clause))))
  LimitClause
  (as-sql [clause]
    (str "LIMIT "(.limit clause)))
  OffsetClause
  (as-sql [clause]
    (str "OFFSET "(.offset clause)))
  AndPredicate
  (as-sql [and-predicate]
    (str (as-sql (.pred1 and-predicate))" AND "(as-sql (.pred2 and-predicate))))
  OrPredicate
  (as-sql [or-predicate]
    (str (as-sql (.pred1 or-predicate))" OR "(as-sql (.pred2 or-predicate))))
  SimplePredicate
  (as-sql [predicate]
    (str (as-sql (.x predicate))" "(name (.operator predicate))" "(as-sql (.y predicate))))
  IsPredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" IS NULL"))
  IsNotPredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" IS NOT NULL"))
  InPredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" IN "(as-sql (.list predicate))))
  BetweenPredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" BETWEEN "(as-sql (.x predicate))" AND "(as-sql (.y predicate))))
  LikePredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" LIKE "(.like predicate)))
  clojure.lang.PersistentVector
  (as-sql [vector] (str "("(str/join ", " (map as-sql vector))")"))
  clojure.lang.Keyword
  (as-sql [keyword] (sanitize-field keyword))
  java.lang.Number
  (as-sql [number] "?")
  java.lang.String
  (as-sql [string] "?"))
