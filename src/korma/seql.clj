(ns korma.seql
  "A new syntax for writing SQL in Korma"
  (:require [clojure.string :as str]
            [korma.sql.engine :as eng]
            [korma.core :refer [default-fk-name]]))

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
    (let [components (.components query)
          existing-clause (:fields components)]
      (if (and existing-clause (not= ['*] (.fields existing-clause)))
        (->Query (assoc components :fields (FieldsClause. (concat (.fields existing-clause) fields))))
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
      (if-let [existing-clause (:order components)]
        (->Query (assoc components :order (OrderClause. (concat (.fields existing-clause) fields) (.direction clause))))
        (->Query (assoc components :order clause))))))

(deftype LimitClause [limit]
  Clause
  (add-to-query [clause query]
    (->Query (assoc (.components query) :limit clause))))

(deftype OffsetClause [offset]
  Clause
  (add-to-query [clause query]
    (->Query (assoc (.components query) :offset clause))))

(deftype GroupClause [fields]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)
          existing-clause (:group components)]
      (if existing-clause
        (->Query (assoc components :group (GroupClause. (concat (.fields existing-clause) fields))))
        (->Query (assoc components :group clause))))))

(deftype AggregateClause [aggregator alias group]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)
          existing-clause (:group components)]
      (if existing-clause
        (->Query (assoc components :group (GroupClause. (concat (.fields existing-clause) fields))))
        (->Query (assoc components :group clause))))))

(deftype JoinsClause [joins]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)]
      (if-let [existing-clause (:joins components)]
        (->Query (assoc components :joins (JoinsClause. (concat (.joins existing-clause) (.joins clause)))))
        (->Query (assoc components :joins clause))))))

(defn qualify-key [entity key]
  (keyword (str (name (or (:alias entity) (:table entity)))"."(name key))))

(deftype WithClause [with]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)]
     (if-let [entity (:entity components)]
       (if-let [rel (get (:rel entity) (:table with))]
         (add-to-query (->JoinsClause [{:type :inner :entity with
                                        :field-a (qualify-key entity (:pk @rel))
                                        :field-b (qualify-key with (:fk @rel))}]) query)
         (throw (Exception. (str "No relationship defined between "(:table entity)" and "(:table with)))))
       (throw (Exception. "Can not use WITH without an Entity"))))))

(deftype SetClause [fields]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)]
      (if-let [existing-clause (:set components)]
        (->Query (assoc components :set (SetClause. (concat (.fields existing-clause) fields))))
        (->Query (assoc components :set clause))))))

(deftype ValuesClause [entities]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)]
      (if-let [existing-clause (:values components)]
        (->Query (assoc components :values (ValuesClause. (concat (.entities existing-clause) entities))))
        (->Query (assoc components :values clause))))))

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
  ;; List is chosen explicitly because it is an _ordered_ collection,
  ;; which should make variable substitution logic deterministic
  java.util.List
  (to-predicate [l] (let [preds (map simple-predicate l)]
                      (if (= (count preds) 1)
                        (first preds)
                        (->AndPredicate preds))))
  clojure.lang.APersistentMap
  (to-predicate [m] (to-predicate (vec m)))
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
          (->Query {:type :select :entity (to-entity entity) :fields (->FieldsClause ['*])}) clauses))

(defn UPDATE [entity & clauses]
  (reduce (fn [query clause] (add-to-query clause query))
          (->Query {:type :update :entity (to-entity entity)}) clauses))

(defn DELETE [entity & clauses]
  (reduce (fn [query clause] (add-to-query clause query))
          (->Query {:type :delete :entity (to-entity entity)}) clauses))

(defn INSERT [entity & clauses]
  (reduce (fn [query clause] (add-to-query clause query))
          (->Query {:type :insert :entity (to-entity entity)}) clauses))

(defn FIELDS [& fields]
  (->FieldsClause fields))

(defn WHERE
  ([predicate] (->WhereClause predicate))
  ([key val & conditions]
     (WHERE (to-predicate (concat [[key val]] (partition 2 conditions))))))

(defn ORDER
  ([direction & fields] (->OrderClause fields direction))
  ([field] (ORDER :asc field)))

(defn LIMIT [limit]
  (->LimitClause limit))

(defn OFFSET [offset]
  (->OffsetClause offset))

(defn WITH [entity]
  (->WithClause (to-entity entity)))

(defn GROUP [& fields]
  (->GroupClause fields))

(defn JOIN
  ([entity] (WITH entity))
  ([entity field-a field-b]
     (->JoinsClause [{:type :inner :entity (to-entity entity) :field-a field-a :field-b field-b}])))

(defn SET [& fields]
  (->SetClause (partition 2 fields)))

(defn VALUES [& entities]
  (->ValuesClause entities))

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
  ([table field] (str (delimit table)"."field))
  ([field] (if *table*
             (qualify-field (name *table*) field)
             field)))

(defn split-on-dots [obj]
  (.split (name obj) "\\."))

(def desc-variants #{:desc "desc" :DESC "DESC"})

(defn to-order [order]
  (if (desc-variants order) "DESC" "ASC"))

(defn clauses-to-sql [components clause-names]
  (str/join " "
            (remove nil?
                    (map #(if-let [component (components %)] (as-sql component) nil)
                         clause-names))))

(def query-names {:select "SELECT" :update "UPDATE" :delete "DELETE FROM" :insert "INSERT INTO"})

(def query-components
  {:select [:fields :entity :joins :where :group #_:having :order :limit :offset]
   :update [:entity :set :where]
   :delete [:entity :where]
   :insert [:entity :values]})

(def join-names {:inner "INNER JOIN"})

(defn special-case-sql [{:keys [type values]}]
  (when (= :insert type)
    (let [non-empty-entities (when (not (nil? values)) (remove empty? (.entities values)))]
      (when (or (nil? non-empty-entities) (empty? non-empty-entities))
        "DO 0"))))

(extend-protocol Sqlable
  Entity
  (as-sql [entity]
    (str/join " " (map delimit (remove nil? [(:table entity) (:alias entity)]))))
  Query
  (as-sql [query]
    (if-let [special (special-case-sql (or (.components query) {}))]
      special
      (let [components (.components query)
            entity (:entity components)
            fields (:fields components)]
        (binding [*table* (or (:alias entity) (:table entity))]
          (str (query-names (:type components))" "
               (clauses-to-sql
                components
                (query-components (:type components))))))))
  FieldsClause
  (as-sql [clause]
    (str (str/join ", " (map as-sql (.fields clause)))" FROM"))
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
  GroupClause
  (as-sql [clause]
    (str "GROUP BY "(str/join ", " (map as-sql (.fields clause)))))
  JoinsClause
  (as-sql [clause]
    (str/join " "
              (map
               (fn [join] (str (join-names (:type join))" "(as-sql (:entity join))" ON "(as-sql (:field-a join))" = "(as-sql (:field-b join))))
               (.joins clause))))
  SetClause
  (as-sql [clause]
    (str "SET "(str/join ", " (map (fn [[field value]] (str (delimit (name field))" = "(as-sql value))) (.fields clause)))))
  ValuesClause
  (as-sql [clause]
    (let [entities (.entities clause)
          fields (distinct (flatten (map keys entities)))]
      (str "("(str/join ", " (map delimit (map name fields)))") VALUES "
           (str/join ", " (map (fn [entity] (str "("(str/join ", " (map as-sql (map entity fields)))")"))
                               entities)))))
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
  (as-sql [keyword] (apply qualify-field
                           (let [parts (split-on-dots keyword)]
                             (concat (butlast parts) [(delimit (last parts))]))))
  clojure.lang.Symbol
  (as-sql [symbol] (apply qualify-field (split-on-dots symbol)))
  java.lang.Number
  (as-sql [number] "?")
  java.lang.String
  (as-sql [string] "?")
  java.lang.Boolean
  (as-sql [bool] (if bool "TRUE" "FALSE"))
  nil
  (as-sql [bool] "NULL"))
