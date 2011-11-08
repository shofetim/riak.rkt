#lang racket/base

(require rackunit
         "main.rkt")

(define-simple-check (check-is-hash? p)
  (hash? p))

(check-equal? (ping) 'OK)
;;(status) ;; Must have riak_kv_stat enabled for this test, otherwise it is a 404
(check-is-hash? (list-resources))


