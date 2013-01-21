;; ASDF package description for view-server              -*- Lisp -*-

(defpackage :view-server-system (:use :cl :asdf))
(in-package :view-server-system)

(defsystem view-server
  :name "CouchDB View Server"
  :maintainer "Kevin Raison"
  :author "Kevin Raison <last name @ chatsubo dot net>"
  :version "0.1"
  :description "view-server"
  :depends-on (:clouchdb :cl-json :cl-syslog :bordeaux-threads)
  :components ((:file "view-server")))
