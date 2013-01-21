(in-package #:cl-user)

(defpackage #:view-server
  (:use #:cl #:clouchdb #:parenscript)
  (:export #:start-view-server
           #:stop-view-server
           #:*view-server-port*
           #:*view-package*
           #:create-lisp-view
           #:ad-hoc-lisp-view
           #:lisp-view
           #:with-doc-slots
           #:@))

(in-package :view-server)
(defvar *view-package* :view-server)
(defvar *view-server-thread* nil)
(defvar *view-server-port* 5478)
(defvar *stop-view-server* nil)
(defvar *view-server-threads* nil)
(defvar *view-server-lock* (bordeaux-threads:make-recursive-lock))
(defvar *maps* nil)

(defmacro with-gensyms (syms &body body)
  `(let ,(loop for s in syms collect `(,s (gensym)))
    ,@body))

(defun logger (level msg &rest args)
  (syslog:log "view-server"
              :local6
              level
              (apply #'format nil msg args)
              syslog:+log-pid+))

(defun emit (x y)
  (list x y))

(defgeneric aget (key alistish)
  (:documentation "Do a `key' lookup in `alistish' and return two
  values: the actual value and whether the lookup was successful (just
  like `gethash' does)."))

(defmethod aget (key (alist list))
  "The value of `key' in `alist'"
  (let ((x (assoc key alist :test #'equal)))
    (values
     (cdr x)
     (consp x))))

(defmethod aget (key (hash hash-table))
  (gethash key hash))

(defmethod (setf aget) (value key (hash hash-table))
  (setf (gethash key hash) value))

(defmethod (setf aget) (value key (alist list))
  (if (null (assoc key alist))
      (progn
        (rplacd alist (copy-alist alist))
        (rplaca alist (cons key value))
        value)
      (cdr (rplacd (assoc key alist) value))))

(defun @ (alist key &rest more-keys)
  "Swiss army knife function for alists and any objects for whom
  `aget' has been defined. It works on alistish objects much much like
  the dot `.' works in many other object oriented
  languages (eg. Python, JavaScript).

  (@ alistish-object :foo :bar :baz) is equivalent to
  calling (aget :baz (aget :bar (aget :foo x))), or
  alistish_object.foo.bar.baz in JS. A setter for `@' is also
  defined.

  It returns two values: the actual value and whether the lookup was
  successful (just like `gethash' does).

  `equal' is used for testing identity for keys."
  (if (null more-keys)
      (aget key alist)
      (apply '@ (aget key alist) more-keys)))

(defun (setf @) (val alist key &rest more-keys)
  (if (null more-keys)
      (setf (aget key alist) val)
      (setf (apply #'@ (aget key alist) more-keys) val)))

(defun add-thread (thread)
  (bordeaux-threads:with-lock-held (*view-server-lock*)
    (pushnew thread *view-server-threads*)))

(defun remove-thread (thread)
  (bordeaux-threads:with-lock-held (*view-server-lock*)
    (setq *view-server-threads*
          (delete thread *view-server-threads*))))

(defun wait-for-input (stream timeout)
  (loop until (listen stream) do
       (if (> (get-universal-time) timeout)
           (progn
             (logger :err "~A timing out!"
                     (bordeaux-threads:current-thread))
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
  (create-view id (clouchdb::string-join view-defs) :language "cl"))

(defun ad-hoc-lisp-view (body)
  (my-ad-hoc-view
   (json:encode-json-to-string (format nil "~S" body))
   :language "cl"))

(defun apply-view-maps (input maps stream)
  (let ((result (mapcar #'(lambda (fn)
                            (funcall fn (second input)))
                        maps)))
    ;;(logger :err "~A RESULT: ~A" *package* result)
    (format stream "[")
    (dotimes (i (length result))
      (let ((r (nth i result)))
        (if (null r)
            (format stream "[[]]")
            (clouchdb::document-to-json-stream (list r) stream)))
      (unless (= i (1- (length result)))
        (format stream ",")))
    (format stream "]~%")))

(defun apply-reduces (input stream)
  ;; ["reduce",["function(k, v) { return sum(v); }"],
  ;; [[[1,"699b524273605d5d3e9d4fd0ff2cb272"],10],
  ;; [[2,"c081d0f69c13d2ce2050d684c7ba2843"],20],[[null,"foobar"],3]]]
  ;;(logger :info "REDUCE: ~A" input)
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
          ;;(logger :info "REDUCE SENDING ~A" json)
          (format stream "~A" json)
          (terpri stream)))
    (error (c)
      (format stream "{\"error\":\"~A\",\"reason\":\"~A: ~A\"}~%"
              1 "reduce error" c))))

(defun apply-rereduces (input stream)
  ;;["rereduce",["function(k, v, r) { return sum(v); }"],[33,55,66]]\n
  ;;(logger :info "REREDUCE: ~A" input)
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
          ;;(logger :info "REREDUCE SENDING ~A" json)
          (format stream "~A" json)
          (terpri stream)))
    (error (c)
      (format stream "{\"error\":\"~A\",\"reason\":\"~A: ~A\"}~%"
              1 "rereduce error" c))))

(defun process-request (request stream)
  (logger :err "view-server read: ~A" request)
  (let ((input (json-to-document request)))
    (cond ((equalp (car input) "reset")
           (setq *maps* nil)
           (format stream "true~%"))
          ((equalp (car input) "add_fun")
           (handler-case
               (setq *maps*
                     (append *maps*
                             (list
                              (eval (read-from-string
                                     (second input))))))
             (:no-error (r)
               (declare (ignore r))
               ;;(logger :info "~A MAPS: ~A" *package* *maps*)
               (format stream "true~%"))
             (error (c)
               (logger :info "VIEW-SERVER: ~A" c)
               (format stream "{\"error\":\"~A\",\"reason\":\"~A: ~A\"}~%"
                       1 "bad lisp" c))))
          ((equalp (car input) "map_doc")
           ;;(logger :err "mapping doc: ~A" (@ (second input) :|_id|))
           (apply-view-maps input *maps* stream))
          ((equalp (car input) "reduce")
           ;;(logger :info "reducing doc: ~A" (second input))
           (apply-reduces input stream))
          ((equalp (car input) "rereduce")
           ;;(logger :info "rereducing doc: ~A" (second input))
           (apply-rereduces input stream))
          (t
           (logger :err "Don't know hot to handle ~A" input))))
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
  (bordeaux-threads:make-thread
   #'(lambda ()
       (logger :debug "IN ACCEPT-HANDLER FOR ~A" socket)
       (unwind-protect
            (handler-case
                (let ((stream (usocket:socket-stream socket)))
                  (view-server-loop stream))
              (end-of-file (c)
                (logger :err "~A got EOF on ~A"
                        (bordeaux-threads:current-thread) socket)
                (sb-thread:return-from-thread c)))
         (progn
           (logger :debug "terminating ~A" socket)
           (ignore-errors (usocket:socket-close socket))
           (remove-thread (bordeaux-threads:current-thread)))))
  :name (format nil "~A handler" socket)))

(defun start-view-server (&key (address "127.0.0.1") (port *view-server-port*))
  (setq *stop-view-server* nil
        *view-server-thread*
        (bordeaux-threads:make-thread
         #'(lambda ()
             (logger :info "Starting view-server on port ~A" port)
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
                                (logger :err "View-server got error on ~A: ~A"
                                        listener c)))))
                      (error (c)
                        (logger :err
                                "UNHANDLED ERROR OF TYPE ~A IN VIEW-SERVER: ~A"
                                (type-of c) c)))))
             (logger :info "Shutting down view-server on port ~A" port)))))

(defun stop-view-server ()
  (when (bordeaux-threads:threadp *view-server-thread*)
    (setq *stop-view-server* t)
    (loop until (not (bordeaux-threads:thread-alive-p *view-server-thread*))
       do (sleep 0.1)))
  (setq *view-server-thread* nil))
