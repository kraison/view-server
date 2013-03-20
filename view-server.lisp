(in-package #:cl-user)

(defpackage #:view-server
  (:use #:cl #:clouchdb #:parenscript #:bordeaux-threads)
  (:export #:start-view-server
           #:stop-view-server
           #:*view-server-port*
           #:*view-package*
           #:create-lisp-view
           #:ad-hoc-lisp-view
           #:lisp-view
           #:with-doc-slots
           #:emit
           #:@))

(in-package :view-server)
(defvar *view-package* :view-server)
(defvar *view-server-thread* nil)
(defvar *view-server-port* 5477)
(defvar *stop-view-server* nil)
(defvar *view-server-threads* nil)
(defvar *view-server-lock* (make-recursive-lock))
(defparameter *in-maps* nil)
(defparameter *map-results* nil)
(defvar *maps* nil)

(defmacro with-gensyms (syms &body body)
  `(let ,(loop for s in syms collect `(,s (gensym)))
    ,@body))

(defun emit (x y)
  (if view-server::*in-maps*
      (push (list x y) view-server::*map-results*)
      (list x y)))

(defun add-thread (thread)
  (with-lock-held (*view-server-lock*)
    (pushnew thread *view-server-threads*)))

(defun remove-thread (thread)
  (with-lock-held (*view-server-lock*)
    (setq *view-server-threads*
          (delete thread *view-server-threads*))))

(defun wait-for-input (stream timeout)
  (loop until (listen stream) do
       (if (> (get-universal-time) timeout)
           (progn
             (log:error "~A timing out!" (current-thread))
             (sb-thread:return-from-thread "timeout!"))
           (sleep 0.01))))

(defmacro with-doc-slots (slots doc &body body)
  (with-gensyms (d)
    `(let ((,d ,doc))
       (let (,@(mapcar
                #'(lambda (slot-sym)
                    (cond ((eql slot-sym 'id)
                           `(id (@ ,d :|_id|)))
                          ((eql slot-sym 'rev)
                           `(rev (@ ,d :|_rev|)))
                          (t
                           (let ((key
                                  (intern
                                   (string-upcase (symbol-name slot-sym))
                                   :keyword)))
                             `(,slot-sym (@ ,d ,key))))))
                slots))
         ,@body))))

(defmacro lisp-view ((&optional view-name) &body body)
  `(let ((vht (make-hash-table :test 'equalp))
         (ht (make-hash-table :test 'equalp)))
     ,@(mapcar #'(lambda (def)
                   `(setf (gethash ,(string-downcase
                                     (format nil "~A" (first def))) ht)
                          ,(format nil "~S" (second def))))
               body)
     (setf (gethash ,view-name vht) ht)
     (format nil "\"~A\":~A" ,view-name
             (json:encode-json-to-string ht))))

(defun create-lisp-view (id &rest view-defs)
  "Create one or more views in the specified view document ID."
  (create-view id (clouchdb::string-join view-defs) :language "lisp"))

(defun my-ad-hoc-view (view &rest options &key key start-key
                       start-key-docid end-key end-key-docid limit stale
                       descending skip group group-level reduce
                       include-docs (language "lisp"))
  "Execute query using an ad-hoc view."
  (declare (ignore key start-key start-key-docid end-key end-key-docid
                   limit stale descending skip group group-level
                   reduce include-docs))
  (clouchdb::ensure-db ()
    (clouchdb::db-request (clouchdb::cat
                           (clouchdb::url-encode (clouchdb::db-name *couchdb*))
                           "/_temp_view")
                          :method :post
                          :external-format-out clouchdb::+utf-8+
                          :content-type "application/json"
                          :content-length nil
                          :parameters (clouchdb::transform-params
                                       options clouchdb::*view-options*)
                          :content
                          (clouchdb::cat "{\"language\" : \"" language "\","
                                         "\"map\" : " view "}"))))

(defun ad-hoc-lisp-view (body)
  (my-ad-hoc-view
   (json:encode-json-to-string (format nil "~S" body))
   :language "lisp"))

(defun apply-view-maps (input maps stream)
  (let ((result (mapcar #'(lambda (fn)
                            (let ((view-server::*map-results* nil)
                                  (view-server::*in-maps* t))
                              (funcall fn (second input))
                              (reverse view-server::*map-results*)))
                        maps)))
    (log:debug "~A RESULT: ~A" *package* result)
    (format stream "[")
    (dotimes (i (length result))
      (let ((r (nth i result)))
        (if (null r)
            (format stream "[[]]")
            (clouchdb::document-to-json-stream r stream)))
      (unless (= i (1- (length result)))
        (format stream ",")))
    (format stream "]~%")))

(defun apply-reduces (input stream)
  ;; ["reduce",["function(k, v) { return sum(v); }"],
  ;; [[[1,"699b524273605d5d3e9d4fd0ff2cb272"],10],
  ;; [[2,"c081d0f69c13d2ce2050d684c7ba2843"],20],[[null,"foobar"],3]]]
  (log:debug "REDUCE: ~A" input)
  (handler-case
      (let ((fns (mapcar #'(lambda (fn)
                             (eval (read-from-string fn)))
                         (second input)))
            (keys (mapcar #'first (third input)))
            (data (mapcar #'second (third input))))
        (let ((json
               (clouchdb::document-to-json
                (list t
                      (mapcar #'(lambda (fn)
                                  (funcall fn keys data nil))
                              fns)))))
          (log:debug "REDUCE SENDING ~A" json)
          (format stream "~A" json)
          (terpri stream)))
    (error (c)
      (format stream "{\"error\":\"~A\",\"reason\":\"~A: ~A\"}~%"
              1 "reduce error" c))))

(defun apply-rereduces (input stream)
  ;;["rereduce",["function(k, v, r) { return sum(v); }"],[33,55,66]]\n
  (log:debug "REREDUCE: ~A" input)
  (handler-case
      (let ((fns (mapcar #'(lambda (fn)
                             (eval (read-from-string fn)))
                         (second input)))
            (data (third input)))
        (let ((json
               (clouchdb::document-to-json
                (list t
                      (mapcar #'(lambda (fn)
                                  (funcall fn nil data t))
                              fns)))))
          (log:debug "REREDUCE SENDING ~A" json)
          (format stream "~A" json)
          (terpri stream)))
    (error (c)
      (format stream "{\"error\":\"~A\",\"reason\":\"~A: ~A\"}~%"
              1 "rereduce error" c))))

(defun process-request (request stream)
  (log:debug "view-server read: ~A" request)
  (let ((input (json-to-document request)))
    (cond ((equalp (car input) "reset")
           (setq *maps* nil)
           (format stream "true~%"))
          ((equalp (car input) "add_fun")
           (handler-case
               (push (eval (read-from-string (second input))) *maps*)
             (:no-error (r)
               (declare (ignore r))
               (log:debug "~A MAPS: ~A" *package* *maps*)
               (format stream "true~%"))
             (error (c)
               (log:debug "VIEW-SERVER: ~A" c)
               (format stream "{\"error\":\"~A\",\"reason\":\"~A: ~A\"}~%"
                       1 "bad lisp" c))))
          ((equalp (car input) "map_doc")
           (log:debug "mapping doc: ~A" (@ (second input) :|_id|))
           (apply-view-maps input (reverse *maps*) stream))
          ((equalp (car input) "reduce")
           (log:debug "reducing doc: ~A" (second input))
           (apply-reduces input stream))
          ((equalp (car input) "rereduce")
           (log:debug "rereducing doc: ~A" (second input))
           (apply-rereduces input stream))
          ((equalp (car input) "ddoc")
           (log:debug "Got design doc: ~D" input)
           nil)
          (t
           (log:error "Don't know hot to handle ~A" input)
           nil)))
  (force-output stream))

(defun view-server-loop (stream)
  (let ((timeout (+ 10 (get-universal-time)))
        (*maps* nil)
        (*package* (find-package *view-package*)))
    (loop
       (wait-for-input stream timeout)
       (let ((request (read-line stream)))
         (setq timeout (+ 10 (get-universal-time)))
         (process-request request stream)))))

(defun accept-handler (socket)
  (make-thread
   #'(lambda ()
       (log:debug "IN ACCEPT-HANDLER FOR ~A" socket)
       (unwind-protect
            (handler-case
                (let ((stream (usocket:socket-stream socket)))
                  (view-server-loop stream))
              (end-of-file (c)
                (log:error "~A got EOF on ~A" (current-thread) socket)
                (sb-thread:return-from-thread c)))
         (progn
           (log:debug "accept-handler terminating ~A" socket)
           (ignore-errors (usocket:socket-close socket))
           (remove-thread (current-thread)))))
  :name (format nil "~A handler" socket)))

(defun start-view-server (&key (address "127.0.0.1") (port *view-server-port*))
  (setq *stop-view-server* nil
        *view-server-thread*
        (make-thread
         #'(lambda ()
             (log:info "Starting view-server on port ~A" port)
             (usocket:with-server-socket (listener (usocket:socket-listen
                                                    address port :reuse-address t))
               (loop until *stop-view-server* do
                    (handler-case
                        (when (usocket:wait-for-input listener
                                                      :ready-only t :timeout 1)
                          (let ((client-connection
                                 (usocket:socket-accept listener)))
                            (handler-case
                                (let ((thread (accept-handler client-connection)))
                                  (add-thread thread))
                              (usocket:connection-aborted-error ())
                              (usocket:socket-error (c)
                                (log:error "View-server got error on ~A: ~A"
                                        listener c)))))
                      (error (c)
                        (log:error
                                "UNHANDLED ERROR OF TYPE ~A IN VIEW-SERVER: ~A"
                                (type-of c) c)))))
             (log:info "Shutting down view-server on port ~A" port))
         :name "view-server-thread")))

(defun stop-view-server ()
  (when (threadp *view-server-thread*)
    (setq *stop-view-server* t)
    (loop until (not (thread-alive-p *view-server-thread*))
       do (sleep 0.1)))
  (setq *view-server-thread* nil))
