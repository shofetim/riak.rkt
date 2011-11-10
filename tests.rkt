#lang racket/base

(require rackunit
         "main.rkt")

(define-simple-check (check-is-hash? p)
  (hash? p))

(define-simple-check (check-is-key? p)
  (string? p))

;; (check-equal? (ping) "\"OK\"")
;; ;;(status) ;; Must have riak_kv_stat enabled for this test, otherwise it is a 404
;; (check-is-hash? (list-resources))
;; (check-is-hash? (list-buckets))
;; (check-is-hash? (list-keys "test"))
;; (check-is-hash? (get-bucket "test"))
;; (check-equal? (put-bucket "test" (hasheq 'props (hasheq 'n_val 5))) '())
;; (check-equal? (get-object "test" "Hr05PhC5XRAtaWSGuBDCVU1T72c") "\"this is a test\"")
;;(check-is-key? (post-object "test" (hasheq 'isTest? #t)))
(check-equal? ((λ () 
                   (put-object "test" "this-is-a-key" (hasheq 'isTest? #t))
                   (get-object "test" "this-is-a-key")))
              #hasheq((isTest? . #t)))

