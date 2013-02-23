(ns korma.seql
  "A new syntax for writing SQL in Korma"
  (:require [clojure.string :as str]
            [korma.sql.engine :as eng]))

(defrecord Entity [table name pk db transforms prepares fields rel])

(defn create-entity
  "Create an entity representing a table in a database."
  [table]
  (map->Entity
   {:table table
    :name table
    :pk :id
    :db nil
    :transforms '()
    :prepares '()
    :fields []
    :rel {}}))

(defprotocol ToEntity
  (to-entity [obj]))

(extend-protocol ToEntity
  Entity
  (to-entity [entity] entity)
  String
  (to-entity [name] (create-entity name))
  clojure.lang.Keyword
  (to-entity [kw] (create-entity (name kw))))

(defmacro defentity
  "Define an entity representing a table in the database, applying any modifications in
  the body."
  [ent & body]
  `(let [e# (-> (create-entity ~(name ent))
                ~@body)]
     (def ~ent e#)))

(deftype Query [components]
  clojure.lang.Seqable
  (seq [this] (prn components)))

(defprotocol Clause
  (add-to-query [clause query]))

(deftype AndPredicate [predicates])

(deftype OrPredicate [predicates])

(deftype SimplePredicate [operator field value])

(deftype IsPredicate [field value])

(deftype IsNotPredicate [field value])

(deftype InPredicate [field list])

(deftype BetweenPredicate [field min max])

(deftype LikePredicate [field like])

(deftype NotPredicate [predicate])

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
        (->Query (assoc components :where (WhereClause. (->AndPredicate [(.predicate existing-where)
                                                                         (.predicate where)]))))
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

;;; predicates

(defn =-predicate [field value]
  (->SimplePredicate "=" field value))

(defn >-predicate [field value]
  (->SimplePredicate ">" field value))

(defn <-predicate [field value]
  (->SimplePredicate "<" field value))

(defn >=-predicate [field value]
  (->SimplePredicate ">=" field value))

(defn <=-predicate [field value]
  (->SimplePredicate "<=" field value))

(defn not=-predicate [field value]
  (->NotPredicate (->SimplePredicate "=" field value)))

(defn not-in-predicate [field value]
  (->NotPredicate (->InPredicate field value)))

(def raw-predicates {'like ->LikePredicate
                     'not ->IsNotPredicate
                     'in ->InPredicate
                     'not-in not-in-predicate
                     'between ->BetweenPredicate
                     '> >-predicate
                     '< <-predicate
                     '>= >=-predicate
                     '<= <=-predicate
                     'not= not=-predicate
                     '= =-predicate})

(def predicates (reduce (fn [h [k v]] (assoc h (keyword k) v)) raw-predicates raw-predicates))

(def predicate-names (set (keys predicates)))

(defprotocol ParseCondition
  (parse-condition [condition key]))

(extend-protocol ParseCondition
  clojure.lang.PersistentVector
  (parse-condition [condition field]
    (let [[name & args] condition]
      (if (predicate-names name)
        (apply (predicates name) field args)
        (->InPredicate field condition))))
  java.lang.Object
  (parse-condition [condition key] (=-predicate key condition)))

(defn simple-predicate
  ([key condition] (parse-condition condition key))
  ([[key condition]] (simple-predicate key condition)))

(defprotocol ToPredicate
  (to-predicate [object]))

(extend-protocol ToPredicate
  clojure.lang.APersistentMap
  (to-predicate [m] (let [preds (map simple-predicate m)]
                      (if (= (count preds) 1)
                        (first preds)
                        (->AndPredicate preds))))
  AndPredicate
  (to-predicate [x] x)
  OrPredicate
  (to-predicate [x] x)
  SimplePredicate
  (to-predicate [x] x)
  IsPredicate
  (to-predicate [x] x)
  IsNotPredicate
  (to-predicate [x] x)
  InPredicate
  (to-predicate [x] x)
  BetweenPredicate
  (to-predicate [x] x)
  LikePredicate
  (to-predicate [x] x)
  NotPredicate
  (to-predicate [x] x))


;;; dsl

(defn SELECT [entity & clauses]
  (reduce (fn [query clause] (add-to-query clause query))
          (->Query {:type :select :entity (to-entity entity)}) clauses))

(defn FIELDS [& fields]
  (->FieldsClause fields))

(defn WHERE
  ([predicate] (->WhereClause predicate))
  ([key val & {:as conditions}]
     (WHERE (to-predicate (assoc conditions key val)))))

(defn ORDER
  ([direction & fields] (->OrderClause fields direction))
  ([field] (ORDER :asc field)))

(defn LIMIT [limit]
  (->LimitClause limit))

(defn OFFSET [offset]
  (->OffsetClause offset))

(defn OR [& preds]
  (->OrPredicate (map to-predicate preds)))

(defn AND [& preds]
  (->AndPredicate (map to-predicate preds)))

(def LIKE ->LikePredicate)


;; Sqlable ;;


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

(def desc-variants #{:desc "desc" :DESC "DESC"})

(defn to-order [order]
  (if (desc-variants order) "DESC" "ASC"))

(defn clauses-to-sql [components clause-names]
  (str/join " "
            (remove nil?
                    (map #(if-let [component (components %)] (as-sql component) nil)
                         clause-names))))

(extend-protocol Sqlable
  Entity
  (as-sql [entity]
    (str/join " " (map delimit (remove nil? [(:table entity) (:alias entity)]))))
  Query
  (as-sql [query]
    (let [components (.components query)
          entity (:entity components)
          fields (:fields components)]
      (binding [*table* (or (:alias entity) (:table entity))]
        (str "SELECT "
             (if fields (as-sql fields) (str (delimit (name *table*))".*"))
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
    (str "WHERE "(as-sql (.predicate clause))))
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
    (str "("(str/join " AND " (map as-sql (.predicates and-predicate)))")"))
  OrPredicate
  (as-sql [or-predicate]
    (str "("(str/join " OR " (map as-sql (.predicates or-predicate)))")"))
  SimplePredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" "(name (.operator predicate))" "(as-sql (.value predicate))))
  IsPredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" IS "(as-sql (.value predicate))))
  IsNotPredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" IS NOT "(as-sql (.value predicate))))
  InPredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" IN "(as-sql (.list predicate))))
  BetweenPredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" BETWEEN "(as-sql (.min predicate))" AND "(as-sql (.max predicate))))
  LikePredicate
  (as-sql [predicate]
    (str (as-sql (.field predicate))" LIKE "(as-sql (.like predicate))))
  NotPredicate
  (as-sql [predicate]
    (str "NOT "(as-sql predicate)))
  clojure.lang.PersistentVector
  (as-sql [vector] (str "("(str/join ", " (map as-sql vector))")"))
  clojure.lang.Keyword
  (as-sql [keyword] (sanitize-field keyword))
  java.lang.Number
  (as-sql [number] "?")
  java.lang.String
  (as-sql [string] "?")
  java.lang.Boolean
  (as-sql [bool] (if bool "TRUE" "FALSE"))
  nil
  (as-sql [bool] "NULL"))
