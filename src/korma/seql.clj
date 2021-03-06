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

(defrecord AndPredicate [predicates])

(defrecord OrPredicate [predicates])

(defrecord SimplePredicate [operator field value])

(defrecord IsPredicate [field value])

(defrecord IsNotPredicate [field value])

(defrecord InPredicate [field list])

(defrecord BetweenPredicate [field min max])

(defrecord LikePredicate [field like])

(defrecord NotPredicate [predicate])

(defrecord SqlFunction [name fields])

(defrecord Field [name alias entity])

(defn field [name & {:as options}]
  (map->Field (merge {:delimit true :qualify true} (assoc options :name name))))

(defn strings-to-field
  ([entity-name field-name] (field field-name :entity {:table entity-name}))
  ([field-name] (field field-name)))

(defn split-on-dots [obj]
  (.split (name obj) "\\."))

(defn- nameable-to-field [nameable entity]
  (let [field (apply strings-to-field (split-on-dots nameable))]
    (if entity (assoc field :entity entity) field)))

(defprotocol ToField
  (to-field [object entity]))

(extend-protocol ToField
  clojure.lang.PersistentVector
  (to-field [[name alias] entity] (assoc (to-field name entity) :alias alias))
  clojure.lang.Keyword
  (to-field [kw entity] (assoc (nameable-to-field kw entity) :delimit true))
  clojure.lang.Symbol
  (to-field [sym entity] (assoc (nameable-to-field sym entity) :delimit false :qualify (= sym '*)))
  Field
  (to-field [field entity] (assoc field :entity entity))
  SqlFunction
  (to-field [function entity] function))

(defn merge-fields [existing-clause new-clause]
  (concat (:fields existing-clause)
          (map #(to-field % (or (:entity %) (:entity new-clause))) (:fields new-clause))))

(defrecord FieldsClause [fields]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)
          existing-clause (:fields components)]
      (if (and existing-clause (.fields existing-clause))
        (->Query (assoc components :fields (FieldsClause. (merge-fields existing-clause clause))))
        (->Query (assoc components :fields clause))))))

(defn assoc-entity [new old]
  (assoc new :entity (:entity old)))

(defrecord WhereClause [predicate]
  Clause
  (add-to-query [where query]
    (let [components (.components query)]
      (if-let [existing-where (:where components)]
        (->Query (assoc components :where (WhereClause. (->AndPredicate [(assoc-entity (.predicate existing-where) existing-where)
                                                                         (assoc-entity (.predicate where) where)]))))
        (->Query (assoc components :where where))))))

(defrecord HavingClause [predicate]
  Clause
  (add-to-query [having query]
    (let [components (.components query)]
      (if-let [existing-having (:having components)]
        (->Query (assoc components :having (HavingClause. (->AndPredicate [(assoc-entity (.predicate existing-having) existing-having)
                                                                           (assoc-entity (.predicate having) having)]))))
        (->Query (assoc components :having having))))))

(defrecord OrderClause [fields direction]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)]
      (if-let [existing-clause (:order components)]
        (->Query (assoc components :order (OrderClause. (merge-fields existing-clause clause)
                                                        (.direction clause))))
        (->Query (assoc components :order clause))))))

(defrecord LimitClause [limit]
  Clause
  (add-to-query [clause query]
    (->Query (assoc (.components query) :limit clause))))

(defrecord OffsetClause [offset]
  Clause
  (add-to-query [clause query]
    (->Query (assoc (.components query) :offset clause))))

(defrecord GroupClause [fields]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)
          existing-clause (:group components)]
      (if existing-clause
        (->Query (assoc components :group (GroupClause. (merge-fields existing-clause clause))))
        (->Query (assoc components :group clause))))))

(defrecord AggregateClause [aggregator alias group]
  Clause
  (add-to-query [clause query]
    (let [aggregated-query (add-to-query
                            (->FieldsClause [(assoc aggregator :alias alias)])
                            query)]
      (if group
        (add-to-query (->GroupClause [group]) aggregated-query)
        aggregated-query))))

(defn primary-key [entity]
  (field (or (:pk entity) :id) :entity entity))

(defn foreign-key [primary-entity entity]
  (field (str (name (:table primary-entity)) "_id") :entity entity))

(defn complete-join [join primary-entity]
  (merge {:type :inner
          :primary-key (primary-key primary-entity)
          :foreign-key (foreign-key primary-entity (:entity join))} join))

(defrecord JoinsClause [joins]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)
          completed-joins (map #(complete-join % (or (:entity clause) (:entity components)))
                               (.joins clause))]
      (if-let [existing-clause (:joins components)]
        (->Query (assoc components :joins (JoinsClause. (concat (.joins existing-clause)
                                                                completed-joins))))
        (->Query (assoc components :joins (JoinsClause. completed-joins)))))))

(defn qualify-key [entity key]
  (keyword (str (name (or (:alias entity) (:table entity)))"."(name key))))

(defn add-all-to-query [clauses query]
  (reduce (fn [q c] (add-to-query c q)) query clauses))

(defn rel-to-join [rel primary-entity foreign-entity]
  (merge
   {:type :inner :entity foreign-entity}
   (case (:rel-type rel)
     :has-one {:primary-key (field (:pk rel) :entity primary-entity)
               :foreign-key (field (:fk rel) :entity foreign-entity)}
     :belongs-to {:primary-key (field (:pk rel) :entity foreign-entity)
                  :foreign-key (field (:fk rel) :entity primary-entity)})))

(defrecord WithClause [with-entity clauses]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)]
      (if-let [entity (or (:entity clause) (:entity components))]
        (if-let [rel (get (:rel entity) (:table with-entity))]
          (add-all-to-query
           (concat
            [(->JoinsClause [(rel-to-join @rel entity with-entity)])
             (->FieldsClause [(field "*" :entity with-entity :delimit false)])]
            (map #(assoc % :entity with-entity) clauses))
           query)
          (throw (Exception. (str "No relationship defined between "(:table entity)" and "(:table with-entity)))))
        (throw (Exception. "Can not use WITH without an Entity"))))))

(defrecord SetClause [fields]
  Clause
  (add-to-query [clause query]
    (let [components (.components query)]
      (if-let [existing-clause (:set components)]
        (->Query (assoc components :set (SetClause. (concat (.fields existing-clause) fields))))
        (->Query (assoc components :set clause))))))

(defrecord ValuesClause [entities]
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
          (->Query {:type :select :entity (to-entity entity) :fields (->FieldsClause nil)}) clauses))

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
  (->FieldsClause (map #(to-field % nil) fields)))

(defn WHERE
  ([predicate] (->WhereClause predicate))
  ([key val & conditions]
     (WHERE (to-predicate (concat [[key val]] (partition 2 conditions))))))

(defn HAVING
  ([predicate] (->HavingClause predicate))
  ([key val & conditions]
     (HAVING (to-predicate (concat [[key val]] (partition 2 conditions))))))

(defn ORDER
  ([direction & fields] (->OrderClause fields direction))
  ([field] (ORDER :asc field)))

(defn LIMIT [limit]
  (->LimitClause limit))

(defn OFFSET [offset]
  (->OffsetClause offset))

(defn WITH [entity & clauses]
  (->WithClause (to-entity entity) clauses))

(defn GROUP [& fields]
  (->GroupClause fields))

(defn COUNT [field]
  (->SqlFunction "COUNT" [field]))

(defn NOW []
  (->SqlFunction "NOW" []))

(defn MAX [field]
  (->SqlFunction "MAX" [field]))

(defn SUM [& fields]
  (->SqlFunction "SUM" fields))

(defn AVG [& fields]
  (->SqlFunction "AVG" fields))

(defn AGGREGATE [function alias & [group-by]]
  (->AggregateClause function alias group-by))

(defn JOIN
  ([entity] (->JoinsClause [{:type :inner :entity (to-entity entity)}]))
  ([entity primary-key foreign-key]
     (->JoinsClause [{:type :inner :entity (to-entity entity)
                      :primary-key primary-key :foreign-key foreign-key}])))

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
  (as-sql [sqlable])
  (as-params [sqlable]))

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

(def desc-variants #{:desc "desc" :DESC "DESC"})

(defn to-order [order]
  (if (desc-variants order) "DESC" "ASC"))

(defn clauses-to-sql [components clause-names]
  (str/join " "
            (remove nil?
                    (map #(if-let [component (components %)] (as-sql component) nil)
                         clause-names))))

(defn clauses-to-params [components clause-names]
  (apply concat (map #(as-params (components %)) clause-names)))

(def query-names {:select "SELECT" :update "UPDATE" :delete "DELETE FROM" :insert "INSERT INTO"})

(def query-components
  {:select [:fields :entity :joins :where :group :having :order :limit :offset]
   :update [:entity :set :where]
   :delete [:entity :where]
   :insert [:entity :values]})

(def join-names {:inner "INNER JOIN"})

(defn special-case-sql [{:keys [type values]}]
  (when (= :insert type)
    (let [non-empty-entities (when (not (nil? values)) (remove empty? (.entities values)))]
      (when (or (nil? non-empty-entities) (empty? non-empty-entities))
        "DO 0"))))

(defmacro with-entity-context [object & body]
  `(if-let [entity# (:entity ~object)]
     (binding [*table* (or (:alias entity#) (:table entity#))]
       ~@body)
     (do ~@body)))

(defn alias-as-sql [alias]
  (when alias (str " AS "(delimit (name alias)))))

(defn normalize-field-table [field]
  (to-field field (or (:entity field) {:table *table*})))

(defn explicit-field-tables [fields]
  (set (map #(name (:table (:entity %)))
            (filter :entity
                    (remove #(= "*" (name (:name %))) fields)))))

(defn remove-extra-star-fields [fields]
  (let [tables-with-explicit-fields (explicit-field-tables fields)]
    (remove #(and (tables-with-explicit-fields (:table (:entity %))) (= "*" (name (:name %))))
            fields)))

(defn normalize-fields [fields]
  (remove-extra-star-fields
   (map normalize-field-table fields)))

(extend-protocol Sqlable
  Entity
  (as-sql [entity]
    (str/join " " (map delimit (remove nil? [(:table entity) (:alias entity)]))))
  (as-params [_] [])
  Query
  (as-sql [query]
    (let [components (or (.components query) {})]
     (if-let [special (special-case-sql components)]
       special
       (with-entity-context components
         (str (query-names (:type components))" "
              (clauses-to-sql
               components
               (query-components (:type components))))))))
  (as-params [query]
    (let [components (or (.components query) {})]
     (if-let [special (special-case-sql components)]
       []
       (clauses-to-params
        components
        (query-components (:type components))))))
  FieldsClause
  (as-sql [clause]
    (with-entity-context clause
      (str
       (if (.fields clause)
         (str/join ", " (distinct (map as-sql (normalize-fields (.fields clause)))))
         (as-sql '*))
       " FROM")))
  (as-params [clause] (apply concat (map as-params (.fields clause))))
  WhereClause
  (as-sql [clause]
    (with-entity-context clause
     (str "WHERE "(as-sql (.predicate clause)))))
  (as-params [clause] (as-params (.predicate clause)))
  HavingClause
  (as-sql [clause]
    (with-entity-context clause
     (str "HAVING "(as-sql (.predicate clause)))))
  (as-params [clause] (as-params (.predicate clause)))
  OrderClause
  (as-sql [clause]
    (with-entity-context clause
     (str "ORDER BY "(str/join ", " (map as-sql (.fields clause)))" "(to-order (.direction clause)))))
  (as-params [_] [])
  LimitClause
  (as-sql [clause]
    (str "LIMIT "(.limit clause)))
  (as-params [_] [])
  OffsetClause
  (as-sql [clause]
    (str "OFFSET "(.offset clause)))
  (as-params [_] [])
  GroupClause
  (as-sql [clause]
    (with-entity-context clause
     (str "GROUP BY "(str/join ", " (map as-sql (.fields clause))))))
  (as-params [_] [])
  JoinsClause
  (as-sql [clause]
    (with-entity-context clause
     (str/join " "
               (map
                (fn [join] (str (join-names (:type join))" "(as-sql (:entity join))" ON "(as-sql (:primary-key join))" = "(as-sql (:foreign-key join))))
                (.joins clause)))))
  (as-params [_] [])
  SetClause
  (as-sql [clause]
    (str "SET "(str/join ", " (map (fn [[field value]] (str (delimit (name field))" = "(as-sql value))) (.fields clause)))))
  (as-params [clause]
    (apply concat (map as-params (map second (.fields clause)))))

  ValuesClause
  (as-sql [clause]
    (let [entities (.entities clause)
          fields (distinct (flatten (map keys entities)))]
      (str "("(str/join ", " (map delimit (map name fields)))") VALUES "
           (str/join ", " (map (fn [entity] (str "("(str/join ", " (map as-sql (map entity fields)))")"))
                               entities)))))
  (as-params [clause] (apply concat (map as-params (apply concat (map vals (.entities clause))))))
  AndPredicate
  (as-sql [and-predicate]
    (str "("(str/join " AND " (map as-sql (.predicates and-predicate)))")"))
  (as-params [predicate] (apply concat (map as-params (.predicates predicate))))
  OrPredicate
  (as-sql [or-predicate]
    (str "("(str/join " OR " (map as-sql (.predicates or-predicate)))")"))
  (as-params [predicate] (apply concat (map as-params (.predicates predicate))))
  SimplePredicate
  (as-sql [predicate]
    (with-entity-context predicate
     (str (as-sql (.field predicate))" "(name (.operator predicate))" "(as-sql (.value predicate)))))
  (as-params [predicate] (as-params (.value predicate)))
  IsPredicate
  (as-sql [predicate]
    (with-entity-context predicate
     (str (as-sql (.field predicate))" IS "(as-sql (.value predicate)))))
  (as-params [_] [])
  IsNotPredicate
  (as-sql [predicate]
    (with-entity-context predicate
     (str (as-sql (.field predicate))" IS NOT "(as-sql (.value predicate)))))
  (as-params [_] [])
  InPredicate
  (as-sql [predicate]
    (with-entity-context predicate
     (str (as-sql (.field predicate))" IN "(as-sql (.list predicate)))))
  (as-params [predicate] (apply concat (map as-params (.list predicate))))
  BetweenPredicate
  (as-sql [predicate]
    (with-entity-context predicate
     (str (as-sql (.field predicate))" BETWEEN "(as-sql (.min predicate))" AND "(as-sql (.max predicate)))))
  (as-params [predicate] (concat (as-params (.min predicate)) (as-params (.max predicate))))
  LikePredicate
  (as-sql [predicate]
    (with-entity-context predicate
     (str (as-sql (.field predicate))" LIKE "(as-sql (.like predicate)))))
  (as-params [predicate] (as-params (.like predicate)))
  NotPredicate
  (as-sql [predicate]
    (str "NOT "(as-sql predicate)))
  (as-params [predicate] (as-params predicate))
  SqlFunction
  (as-sql [function]
    (str (.name function)"("(str/join ", " (map as-sql (.fields function)))")"
         (alias-as-sql (:alias function))))
  (as-params [function] (apply concat (map as-params (.fields function))))
  Field
  (as-sql [field]
    (with-entity-context field
      (let [field-name (name (.name field))
            sql-field-name (if (:delimit field) (delimit field-name) field-name)
            sql-table (when (and (:qualify field) *table*) (str (delimit (name *table*)) "."))]
        (str sql-table sql-field-name (alias-as-sql (.alias field))))))
  (as-params [function] [])
  clojure.lang.PersistentVector
  (as-sql [vector] (str "("(str/join ", " (map as-sql vector))")"))
  (as-params [vector] (apply concat (map as-params vector)))
  clojure.lang.Keyword
  (as-sql [kw] (as-sql (to-field kw nil)))
  (as-params [_] [])
  clojure.lang.Symbol
  (as-sql [sym] (as-sql (to-field sym nil)))
  (as-params [_] [])
  java.lang.Number
  (as-sql [number] "?")
  (as-params [number] [number])
  java.lang.String
  (as-sql [string] "?")
  (as-params [string] [string])
  java.lang.Boolean
  (as-sql [bool] (if bool "TRUE" "FALSE"))
  (as-params [_] [])
  nil
  (as-sql [_] "NULL")
  (as-params [_] []))
