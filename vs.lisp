(ql:quickload :srn)
(srn::start-server)
(srn::delete-db)
(srn::create-db)
(srn::reload-views)
(srn::populate-currencies "/home/raison/work/srn/src/oms/currency.txt")
(srn::valid-currency? "USD")

