(ns korma.test.seql
  (:require
   [clojure.string :as string]
   [clojure.test :refer :all]
   [korma
    [config :refer :all] [db :refer :all] [seql :refer :all]
    [core :refer [database belongs-to has-many has-one table pk]]])
  (:use clojure.test
        korma.config
        korma.seql
        korma.db))

(defdb test-db-opts (postgres {:db "korma" :user "korma" :password "kormapass" :delimiters "" :naming {:fields string/upper-case}}))
(defdb test-db (postgres {:db "korma" :user "korma" :password "kormapass"}))

(defentity delims
  (database test-db-opts))

(defentity users)
(defentity state)
(defentity address
  (belongs-to state))
(defentity email)

(defentity user2
  (table :users)
  (has-one address)
  (has-many email))

(defentity users-alias
  (table :users :u))

(defentity ^{:private true} blah (pk :cool) (has-many users {:fk :cool_id}))

(deftest test-defentity-accepts-metadata
  (is (= true (:private (meta #'blah)))))

(deftest sqlable
  (testing "keywords are interpreted as field names and are qualified and delimited"
    (is (= "\"hi\"" (as-sql :hi)))
    (is (= "`users`.`hi`" (binding [*table* :users *delimiters* ["`" "`"]] (as-sql :hi))))
    (is (= "`hams`.`there`"(binding [*table* :users *delimiters* ["`" "`"]] (as-sql :hams.there)))))

  (testing "vectors are turned into sql lists"
    (is (= "(?, ?, ?)" (as-sql ["hi", "bye", 1]))))

  )

(deftest where-function
  (is (= "SELECT \"users\".* FROM \"users\" WHERE \"users\".\"username\" = ?"
         (as-sql
          (SELECT "users"
                  (WHERE :username "chris")))))
  (is (= "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"username\" = ? AND \"users\".\"email\" = ?)"
         (as-sql
          (SELECT "users"
                  (WHERE :username "chris")
                  (WHERE :email "chris@example.com"))))))

(deftest select-function
  (is (= "SELECT \"users\".\"id\", \"users\".\"username\" FROM \"users\" WHERE \"users\".\"username\" = ? ORDER BY \"users\".\"created\" ASC LIMIT 5 OFFSET 3"
         (as-sql
          (SELECT "users"
                  (FIELDS :id :username)
                  (WHERE :username "chris")
                  (ORDER :created)
                  (LIMIT 5)
                  (OFFSET 3))))))

(deftest simple-selects
  (are [result query] (= result (as-sql query))
       "SELECT \"users\".* FROM \"users\""
       (SELECT users)
       "SELECT \"u\".* FROM \"users\" \"u\""
       (SELECT users-alias)
       "SELECT \"users\".\"id\", \"users\".\"username\" FROM \"users\""
       (SELECT users
               (FIELDS :id :username))
       "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"username\" = ? AND \"users\".\"email\" = ?)"
       (SELECT users
               (WHERE :username "chris"
                      :email "hey@hey.com"))
       "SELECT \"users\".* FROM \"users\" WHERE \"users\".\"username\" = ? ORDER BY \"users\".\"created\" ASC"
       (SELECT users
               (WHERE :username "chris")
               (ORDER :created))
       "SELECT \"users\".* FROM \"users\" WHERE \"users\".\"active\" = TRUE ORDER BY \"users\".\"created\" ASC LIMIT 5 OFFSET 3"
       (SELECT users
               (WHERE :active true)
               (ORDER :created)
               (LIMIT 5)
               (OFFSET 3))))

(deftest update-function
  (is (= "UPDATE \"users\" SET \"first\" = ?, \"last\" = ? WHERE \"users\".\"id\" = ?"
         (as-sql (UPDATE "users"
                         (SET :first "chris"
                              :last "granger")
                         (WHERE :id 3))))))
(deftest update-queries
  (are [result query] (= result (as-sql query))
       "UPDATE \"users\" SET \"first\" = ?"
       (UPDATE users
               (SET :first "chris"))
       "UPDATE \"users\" SET \"first\" = ? WHERE \"users\".\"id\" = ?"
       (UPDATE users
               (SET :first "chris")
               (WHERE :id 3))
       "UPDATE \"users\" SET \"first\" = ?, \"last\" = ? WHERE \"users\".\"id\" = ?"
       (UPDATE users
               (SET :first "chris"
                    :last "granger")
               (WHERE :id 3))))

(deftest delete-function
  (is (= "DELETE FROM \"users\" WHERE \"users\".\"id\" = ?"
         (as-sql (DELETE "users"
                         (WHERE :id 3))))))

(deftest delete-queries
  (are [result query] (= result (as-sql query))
       "DELETE FROM \"users\""
       (DELETE users)
       "DELETE FROM \"users\" WHERE \"users\".\"id\" = ?"
       (DELETE users
               (WHERE :id 3))))

(deftest insert-function
  (is (= "INSERT INTO \"users\" (\"last\", \"first\") VALUES (?, ?)"
         (as-sql (INSERT "users"
                         (VALUES {:first "chris" :last "granger"})))))

  (testing "WHEN values is empty THEN generates a NOOP SQL statement"
    (is (= "DO 0"
           (as-sql (INSERT "users"
                           (VALUES {})))))))


(deftest insert-queries
  (are [result query] (= result (as-sql query))
       "INSERT INTO \"users\" (\"last\", \"first\") VALUES (?, ?)"
       (INSERT users
               (VALUES {:first "chris" :last "granger"}))
       "INSERT INTO \"users\" (\"last\", \"first\") VALUES (?, ?), (?, ?)"
       (INSERT users
               (VALUES {:first "chris" :last "granger"}
                       {:last "jordan" :first "michael"}))
       "DO 0"
       (INSERT users (VALUES {}))
       "DO 0"
       (INSERT users (VALUES {} {}))))

(deftest complex-where
  (are [query result] (= (as-sql query) result)
       (SELECT users
               (WHERE (OR {:name "chris"}
                          {:name "john"})))
       "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"name\" = ? OR \"users\".\"name\" = ?)"

       (SELECT users
               (WHERE (OR {:name "drew"
                           :last "dreward"}
                          {:email "drew@drew.com"}
                          {:age [:> 10]})))
       "SELECT \"users\".* FROM \"users\" WHERE ((\"users\".\"last\" = ? AND \"users\".\"name\" = ?) OR \"users\".\"email\" = ? OR \"users\".\"age\" > ?)"

       (SELECT users
               (WHERE (OR {:x [:< 5]}
                          (OR {:y [:< 3]}
                              {:z [:> 4]}))))
       "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"x\" < ? OR (\"users\".\"y\" < ? OR \"users\".\"z\" > ?))"

       (SELECT users
               (WHERE :name [:like "chris"]))
       "SELECT \"users\".* FROM \"users\" WHERE \"users\".\"name\" LIKE ?"

       (SELECT users
               (WHERE :name ["travis" "chris"]))
       "SELECT \"users\".* FROM \"users\" WHERE \"users\".\"name\" IN (?, ?)"

       (SELECT users
               (WHERE (OR {:name [:like "chris"]}
                          (LIKE :name "john"))))
       "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"name\" LIKE ? OR \"users\".\"name\" LIKE ?)"))

;; (deftest with-many
;;   (with-out-str
;;     (dry-run
;;       (is (= [{:id 1 :email [{:id 1}]}]
;;              (select user2
;;                      (with email)))))))

(deftest with-one
  (is (= "SELECT \"address\".\"state\", \"users\".\"name\" FROM \"users\" INNER JOIN \"address\" ON \"users\".\"id\" = \"address\".\"users_id\""
         (as-sql (SELECT user2
                         (WITH address)
                         (FIELDS :address.state :name))))))

(deftest join
  (are [result query] (= result (as-sql query))
       "SELECT \"blah\".* FROM \"blah\" INNER JOIN \"cool\" ON \"cool\".\"id\" = \"blah\".\"id\""
       (SELECT :blah (JOIN :cool :cool.id :blah.id))

       "SELECT \"users\".* FROM \"users\" INNER JOIN \"user2\" ON \"users\".\"id\" = \"user2\".\"users_id\" INNER JOIN \"user3\" ON \"users\".\"id\" = \"user3\".\"users_id\""
       (SELECT users
               (JOIN :user2 :users.id :user2.users_id)
               (JOIN :user3 :users.id :user3.users_id))))


(deftest aggregate-group
  (are [result query] (= result (as-sql query))
       "SELECT \"users\".* FROM \"users\" GROUP BY \"users\".\"id\", \"users\".\"name\""
       (SELECT users (GROUP :id :name))
       "SELECT COUNT(\"users\".*) AS \"cnt\" FROM \"users\" GROUP BY \"users\".\"id\""
       (SELECT users (AGGREGATE (COUNT '*) :cnt :id))
       "SELECT COUNT(\"users\".*) AS \"cnt\" FROM \"users\" GROUP BY \"users\".\"id\" HAVING \"users\".\"id\" = ?"
       (SELECT users
               (AGGREGATE (COUNT '*) :cnt :id)
               (HAVING :id 5))))

(deftest quoting
  (is (= "SELECT \"users\".\"testField\", \"users\".\"t!\" FROM \"users\""
         (as-sql (SELECT users (FIELDS :testField :t!))))))

(deftest sqlfns

  (is (= "SELECT NOW() AS \"now\", MAX(\"users\".\"blah\"), AVG(SUM(?, ?), SUM(?, ?)) FROM \"users\" WHERE \"users\".\"time\" >= NOW()"
         (as-sql (SELECT users
                         (FIELDS [(NOW) :now] (MAX :blah) (AVG (SUM 3 4) (SUM 4 5)))
                         (WHERE :time [:>= (NOW)]))))))

(deftest join-ent-directly
  (is (= "SELECT \"users\".* FROM \"users\" INNER JOIN \"address\" ON \"users\".\"id\" = \"address\".\"users_id\""
         (as-sql (SELECT user2
                         (JOIN address))))))

(deftest new-with
  (are [result query] (= result (as-sql query))

       "SELECT \"users\".*, \"address\".\"id\" FROM \"users\" INNER JOIN \"address\" ON \"users\".\"id\" = \"address\".\"users_id\""
       (SELECT user2
               (FIELDS '*)
               (WITH address (FIELDS :id)))

       ;; "SELECT \"users\".*, \"address\".*, \"state\".* FROM \"users\" LEFT JOIN \"address\" ON \"users\".\"id\" = \"address\".\"users_id\" LEFT JOIN \"state\" ON \"state\".\"id\" = \"address\".\"state_id\" WHERE (\"state\".\"state\" = ?) AND (\"address\".\"id\" > ?)"
       ;; (select user2
       ;;         (fields :*)
       ;;         (with address
       ;;           (with state (where {:state "nc"}))
       ;;           (where {:id [> 5]})))

       ;; ;;Ensure that params are still ordered correctly
       ;; ["nc" 5]
       ;; (query-only
       ;;   (:params
       ;;     (select user2
       ;;             (fields :*)
       ;;             (with address
       ;;               (with state (where {:state "nc"}))
       ;;               (where (> :id 5))))))

       ;; ;;Validate has-many executes the second query
       ;; "dry run :: SELECT \"users\".* FROM \"users\" :: []\ndry run :: SELECT \"email\".* FROM \"email\" WHERE \"email\".\"email\" LIKE ? AND (\"email\".\"users_id\" = ?) :: [%@gmail.com 1]\n"
       ;; (dry-run
       ;;   (with-out-str
       ;;     (select user2
       ;;             (with email
       ;;                   (where (like :email "%@gmail.com"))))))
       ))

;; (deftest modifiers
;;   (sql-only
;;    (are [result query] (= result query)
;;         "SELECT DISTINCT \"users\".\"name\" FROM \"users\""
;;         (-> (select* "users")
;;             (fields :name)
;;             (modifier "DISTINCT")
;;             as-sql)
;;         "SELECT TOP 5 \"users\".* FROM \"users\""
;;         (select user2 (modifier "TOP 5")))))


;; (deftest naming-delim-options
;;   (sql-only
;;     (is (= "SELECT DELIMS.* FROM DELIMS"
;;            (select delims)))))

;; (deftest false-set-in-update
;;   (sql-only
;;     (are [result query] (= result query)
;;          "UPDATE \"users\" SET \"blah\" = FALSE"
;;          (update user2 (set-fields {:blah false}))

;;          "UPDATE \"users\" SET \"blah\" = NULL"
;;          (update user2 (set-fields {:blah nil}))

;;          "UPDATE \"users\" SET \"blah\" = TRUE"
;;          (update user2 (set-fields {:blah true})))))

;; (deftest raws
;;   (sql-only
;;     (is "SELECT \"users\".* FROM \"users\" WHERE ROWNUM >= ?"
;;         (= (select user2 (where {(raw "ROWNUM") [>= 5]}))))))

;; (deftest pk-dry-run
;;   (let [result (with-out-str
;;                  (dry-run
;;                    (select blah (with users))))]

;;     (is (= "dry run :: SELECT \"blah\".* FROM \"blah\" :: []\ndry run :: SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"cool_id\" = ?) :: [1]\n"
;;            result))))

;; (deftest subselects
;;   (are [result query] (= result query)
;;        "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"id\" IN (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"age\" > ?)))"
;;        (sql-only
;;          (select users
;;                  (where {:id [in (subselect users
;;                                             (where {:age [> 5]}))]})))

;;        "SELECT \"users\".* FROM \"users\", (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"age\" > ?)) \"u2\""
;;        (sql-only
;;          (select users
;;                  (from [(subselect users
;;                                    (where {:age [> 5]})) :u2])))

;;        "SELECT \"users\".*, (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"age\" > ?)) \"u2\" FROM \"users\""
;;        (sql-only
;;          (select users
;;                  (fields :* [(subselect users
;;                                         (where {:age [> 5]})) :u2])))

;;        [10 5 "%@gmail.com"]
;;        (query-only
;;          (:params
;;            (select users
;;                    (where {:logins [> 10]})
;;                    (where {:id [in (subselect users
;;                                               (where {:age [> 5]}))]})
;;                    (where {:email [like "%@gmail.com"]})))))

;;   ;; test that delimiters are set at the time the top-level select is called
;;   (set-delimiters "#")
;;   (defentity five-year-olds
;;     (table (subselect users (where {:age 5})) :five_year_olds))
;; ;;  (pprint (subselect users (where {:age 5})))
;;   (is (= "SELECT #five_year_olds#.* FROM (SELECT #users#.* FROM #users# WHERE (#users#.#age# = ?)) #five_year_olds#"
;;          (sql-only (select five-year-olds))))
;;   (set-delimiters "`")
;;   (is (= "SELECT `five_year_olds`.* FROM (SELECT `users`.* FROM `users` WHERE (`users`.`age` = ?)) `five_year_olds`"
;;          (sql-only (select five-year-olds))))
;;   (set-delimiters "\""))

;; (deftest select-query-object
;;   (are [query result] (= query result)
;;        "SELECT \"blah\".* FROM \"blah\" WHERE (\"blah\".\"id\" = ?)"
;;        (sql-only (select (-> (select* "blah")
;;                            (where {:id 4}))))))

;; (deftest multiple-aggregates
;;   (defentity the_table)
;;   (is (= "SELECT MIN(\"the_table\".\"date_created\") \"start_date\", MAX(\"the_table\".\"date_created\") \"end_date\" FROM \"the_table\" WHERE (\"the_table\".\"id\" IN (?, ?, ?))"
;;          (sql-only
;;            (-> (select* the_table)
;;              (aggregate (min :date_created) :start_date)
;;              (aggregate (max :date_created) :end_date)
;;              (where {:id [in [1 2 3]]})
;;              (exec))))))

;; (deftest not-in
;;   (defentity the_table)
;;   (is (= "SELECT \"the_table\".* FROM \"the_table\" WHERE (\"the_table\".\"id\" NOT IN (?, ?, ?))"
;;          (sql-only
;;            (-> (select* the_table)
;;              (where {:id [not-in [1 2 3]]})
;;              (exec))))))

;; (deftest subselect-table-prefix
;;   (defentity first_table)
;;   (is (= "SELECT \"first_table\".* FROM \"first_table\" WHERE (\"first_table\".\"first_table_column\" = (SELECT \"second_table\".\"second_table_column\" FROM \"second_table\" WHERE (\"second_table\".\"second_table_column\" = ?)))"
;;          (sql-only
;;            (select first_table
;;                    (where {:first_table_column
;;                            (subselect :second_table
;;                                       (fields :second_table_column)
;;                                       (where {:second_table_column 1}))}))))))

;; (deftest entity-as-subselect
;;   (defentity subsel
;;     (table (subselect "test") :test))

;;   ;;This kind of entity needs and alias.
;;   (is (thrown? Exception
;;                (defentity subsel2
;;                  (table (subselect "test")))))

;;   (are [result query] (= result query)
;;        "SELECT \"test\".* FROM (SELECT \"test\".* FROM \"test\") \"test\""
;;        (sql-only
;;          (select subsel))))

;; (deftest multiple-aliases
;;   (defentity blahblah
;;     (table :blah :bb))

;;   (sql-only
;;     (are [result query] (= result query)
;;          "SELECT \"bb\".* FROM \"blah\" \"bb\" LEFT JOIN \"blah\" \"not-bb\" ON \"bb\".\"cool\" = \"not-bb\".\"cool2\""
;;          (select blahblah (join [blahblah :not-bb] (= :bb.cool :not-bb.cool2))))))

;; (deftest empty-in-clause
;;   (sql-only
;;     (are [result query] (= result query)
;;          "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"cool\" IN (?))"
;;          (select :test (where {:cool [in [1]]}))

;;          "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"cool\" IN (NULL))"
;;          (select :test (where {:cool [in []]})))))


;; (deftest prepare-filter
;;   (defn reverse-strings [values]
;;     (apply merge (for [[k v] values :when (string? v)] {k (apply str (reverse v))})))

;;   (defentity reversed-users
;;     (table :users)
;;     (prepare reverse-strings))

;;   (query-only
;;     (is (= ["sirhc" "regnarg"]
;;            (-> (insert reversed-users (values {:first "chris" :last "granger"})) :params)))))

;; (deftest predicates-used-with-brackets
;;   (sql-only
;;    (are [result query] (= result query)
;;         "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" = ?)"
;;         (select :test (where {:id [= 1]}))
;;         "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" < ?)"
;;         (select :test (where {:id [< 10]}))
;;         "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" <= ?)"
;;         (select :test (where {:id [<= 10]}))
;;         "SELECT \"test\".* FROM \"test\" WHERE ((\"test\".\"id\" BETWEEN ? AND ?))"
;;         (select :test (where {:id [between [1 10]]}))

;;      ;; clearly this is not an intended use of 'or'!
;;         "SELECT \"test\".* FROM \"test\" WHERE ((\"test\".\"id\" OR (?, ?, ?)))"
;;         (select :test (where {:id [or [1 2 3]]}))

;;         "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" NOT IN (?, ?, ?))"
;;         (select :test (where {:id [not-in [1 2 3]]}))
;;         "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" != ?)"
;;         (select :test (where {:id [not= 1]})))))


;; ;;*****************************************************
;; ;; Supporting Postgres' schema and queries covering multiple databases
;; ;;*****************************************************

;; (defentity book-with-db (table :korma.book))
;; (defentity author-with-db (table :other.author) (belongs-to book-with-db))

;; (defentity book-with-schema (table :korma.myschema.book))
;; (defentity author-with-schema (table :korma.otherschema.author) (belongs-to book-with-schema))

;; (deftest dbname-on-tablename
;;   (are [query result] (= query result)
;;        (sql-only
;;          (select author-with-db (with book-with-db)))
;;          "SELECT \"other\".\"author\".*, \"korma\".\"book\".* FROM \"other\".\"author\" LEFT JOIN \"korma\".\"book\" ON \"korma\".\"book\".\"id\" = \"other\".\"author\".\"book_id\""))

;; (deftest schemaname-on-tablename
;;   (are [query result] (= query result)
;;        (sql-only
;;          (select author-with-schema (with book-with-schema)))
;;          "SELECT \"korma\".\"otherschema\".\"author\".*, \"korma\".\"myschema\".\"book\".* FROM \"korma\".\"otherschema\".\"author\" LEFT JOIN \"korma\".\"myschema\".\"book\" ON \"korma\".\"myschema\".\"book\".\"id\" = \"korma\".\"otherschema\".\"author\".\"book_id\""))


;; ;;*****************************************************
;; ;; Many-to-Many relationships
;; ;;*****************************************************

;; (declare mtm1 mtm2)

;; (defentity mtm1
;;   (entity-fields :field1)
;;   (many-to-many mtm2 :mtm1_mtm2 {:lfk :mtm1_id
;;                                  :rfk :mtm2_id}))

;; (defentity mtm2
;;   (entity-fields :field2)
;;   (many-to-many mtm1 :mtm1_mtm2 {:lfk :mtm2_id
;;                                  :rfk :mtm1_id}))

;; (deftest test-many-to-many
;;   (is (= (str "dry run :: SELECT \"mtm2\".* FROM \"mtm2\" :: []\n"
;;               "dry run :: SELECT \"mtm1\".* FROM \"mtm1\" "
;;               "INNER JOIN \"mtm1_mtm2\" ON \"mtm1_mtm2\".\"mtm1_id\" "
;;               "= \"mtm1\".\"id\" "
;;               "WHERE (\"mtm1_mtm2\".\"mtm2_id\" = ?) :: [1]\n")
;;          (with-out-str (dry-run (select mtm2 (with mtm1)))))))

;; (deftest test-many-to-many-reverse
;;   (is (= (str "dry run :: SELECT \"mtm1\".* FROM \"mtm1\" :: []\n"
;;               "dry run :: SELECT \"mtm2\".* FROM \"mtm2\" "
;;               "INNER JOIN \"mtm1_mtm2\" ON \"mtm1_mtm2\".\"mtm2_id\" "
;;               "= \"mtm2\".\"id\" "
;;               "WHERE (\"mtm1_mtm2\".\"mtm1_id\" = ?) :: [1]\n"))
;;       (with-out-str (dry-run (select mtm1 (with mtm2))))))

;; (deftest test-many-to-many-join
;;   (is (= (str "dry run :: SELECT \"mtm2\".* FROM \"mtm2\" "
;;               "LEFT JOIN \"mtm1_mtm2\" "
;;               "ON \"mtm2\".\"id\" = \"mtm1_mtm2\".\"mtm2_id\" "
;;               "LEFT JOIN \"mtm1\" "
;;               "ON \"mtm1_mtm2\".\"mtm1_id\" = \"mtm1\".\"id\" :: []\n")
;;          (with-out-str (dry-run (select mtm2 (join mtm1)))))))

;; (deftest test-many-to-many-join-reverse
;;   (is (= (str "dry run :: SELECT \"mtm1\".* FROM \"mtm1\" "
;;               "LEFT JOIN \"mtm1_mtm2\" "
;;               "ON \"mtm1\".\"id\" = \"mtm1_mtm2\".\"mtm1_id\" "
;;               "LEFT JOIN \"mtm2\" "
;;               "ON \"mtm1_mtm2\".\"mtm2_id\" = \"mtm2\".\"id\" :: []\n")
;;          (with-out-str (dry-run (select mtm1 (join mtm2)))))))

;; ;; Entities with many-to-many relationships using default keys.

;; (declare mtmdk1 mtmdk2)

;; (defentity mtmdk1
;;   (entity-fields :field1)
;;   (many-to-many mtmdk2 :mtmdk1_mtmdk2))

;; (defentity mtmdk2
;;   (entity-fields :field2)
;;   (many-to-many mtmdk1 :mtmdk1_mtmdk2))

;; (deftest many-to-many-default-keys
;;   (is (= (str "dry run :: SELECT \"mtmdk2\".* FROM \"mtmdk2\" :: []\n"
;;               "dry run :: SELECT \"mtmdk1\".* FROM \"mtmdk1\" "
;;               "INNER JOIN \"mtmdk1_mtmdk2\" "
;;               "ON \"mtmdk1_mtmdk2\".\"mtmdk1_id\" = \"mtmdk1\".\"id\" "
;;               "WHERE (\"mtmdk1_mtmdk2\".\"mtmdk2_id\" = ?) :: [1]\n")
;;          (with-out-str (dry-run (select mtmdk2 (with mtmdk1)))))))

;; (deftest many-to-many-default-keys-reverse
;;   (is (= (str "dry run :: SELECT \"mtmdk1\".* FROM \"mtmdk1\" :: []\n"
;;               "dry run :: SELECT \"mtmdk2\".* FROM \"mtmdk2\" "
;;               "INNER JOIN \"mtmdk1_mtmdk2\" "
;;               "ON \"mtmdk1_mtmdk2\".\"mtmdk2_id\" = \"mtmdk2\".\"id\" "
;;               "WHERE (\"mtmdk1_mtmdk2\".\"mtmdk1_id\" = ?) :: [1]\n")
;;          (with-out-str (dry-run (select mtmdk1 (with mtmdk2)))))))

;; (deftest test-many-to-many-default-keys-join
;;   (is (= (str "dry run :: SELECT \"mtm2\".* FROM \"mtm2\" "
;;               "LEFT JOIN \"mtm1_mtm2\" "
;;               "ON \"mtm2\".\"id\" = \"mtm1_mtm2\".\"mtm2_id\" "
;;               "LEFT JOIN \"mtm1\" "
;;               "ON \"mtm1_mtm2\".\"mtm1_id\" = \"mtm1\".\"id\" :: []\n")
;;          (with-out-str (dry-run (select mtm2 (join mtm1)))))))

;; (deftest test-many-to-many-default-keys-join-reverse
;;   (is (= (str "dry run :: SELECT \"mtm1\".* FROM \"mtm1\" "
;;               "LEFT JOIN \"mtm1_mtm2\" "
;;               "ON \"mtm1\".\"id\" = \"mtm1_mtm2\".\"mtm1_id\" "
;;               "LEFT JOIN \"mtm2\" "
;;               "ON \"mtm1_mtm2\".\"mtm2_id\" = \"mtm2\".\"id\" :: []\n")
;;          (with-out-str (dry-run (select mtm1 (join mtm2)))))))


;; ;;*****************************************************
;; ;; Union, Union All, Intersect & Except support
;; ;;*****************************************************

;; (deftest test-union
;;   (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) UNION (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"c\" = ? AND \"state\".\"b\" = ?)) :: [1 3 2]\n"
;;          (with-out-str (dry-run (union (queries (subselect users
;;                                                            (where {:a 1}))
;;                                                 (subselect state
;;                                                            (where {:b 2
;;                                                                    :c 3})))))))))

;; (deftest test-union-all
;;   (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) UNION ALL (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"c\" = ? AND \"state\".\"b\" = ?)) :: [1 3 2]\n"
;;          (with-out-str (dry-run (union-all (queries (subselect users
;;                                                                 (where {:a 1}))
;;                                                     (subselect state
;;                                                                 (where {:b 2
;;                                                                         :c 3})))))))))

;; (deftest test-intersect
;;   (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) INTERSECT (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"c\" = ? AND \"state\".\"b\" = ?)) :: [1 3 2]\n"
;;          (with-out-str (dry-run (intersect (queries (subselect users
;;                                                                 (where {:a 1}))
;;                                                     (subselect state
;;                                                                 (where {:b 2
;;                                                                         :c 3})))))))))

;; (deftest test-order-by-in-union
;;   (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) UNION (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"c\" = ? AND \"state\".\"b\" = ?)) ORDER BY \"a\" ASC :: [1 3 2]\n"
;;          (with-out-str (dry-run (union (queries (subselect users
;;                                                             (where {:a 1}))
;;                                                 (subselect state
;;                                                             (where {:b 2
;;                                                                     :c 3})))
;;                                        (order :a)))))))

;; (deftest test-order-by-in-union-all
;;   (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) UNION ALL (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"c\" = ? AND \"state\".\"b\" = ?)) ORDER BY \"a\" ASC :: [1 3 2]\n"
;;          (with-out-str (dry-run (union-all (queries (subselect users
;;                                                                 (where {:a 1}))
;;                                                     (subselect state
;;                                                                 (where {:b 2
;;                                                                         :c 3})))
;;                                            (order :a)))))))

;; (deftest test-order-by-in-intersect
;;   (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) INTERSECT (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"c\" = ? AND \"state\".\"b\" = ?)) ORDER BY \"a\" ASC :: [1 3 2]\n"
;;          (with-out-str (dry-run (intersect (queries (subselect users
;;                                                                 (where {:a 1}))
;;                                                     (subselect state
;;                                                                 (where {:b 2
;;                                                                         :c 3})))
;;                                            (order :a)))))))

