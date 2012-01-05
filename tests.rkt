#lang racket/base

(require rackunit
         "main.rkt")

;;Define additional checks
(define-simple-check (check-is-hash? p)
  (hash? p))

(define-simple-check (check-is-key? p)
  (string? p))

;;Helper functions
(define (get-body ahash)
  (hash-ref ahash 'body))

;;;;Tests
;;Low level API
(check-is-hash? (get-body (status)))    ; Must have riak_kv_stat enabled
(check-equal? (get-body (ping)) "OK")
(check-is-hash? (get-body (list-resources)))
(check-is-hash? (get-body (list-resources 'json)))
(check-pred string? (get-body (list-resources 'text)))
(check-equal? (get-body (put-bucket "test" (hasheq 'props (hasheq 'n_val 5)))) '())
(check-is-hash? (get-body (list-buckets)))
(check-is-hash? (get-body (list-buckets)))
(check-equal? (get-body (put-object "test" "Example" '())) '())
(check-is-hash? (get-body (list-keys "test")))
(check-is-hash? (get-body (get-bucket "test")))
(check-is-hash? (hash-ref (get-body (get-bucket "test")) 'props))
(check-equal? ((λ ()
                  (put-object "test" "this-is-a-key" (hasheq 'isTest? #t))
                  (get-body (get-object "test" "this-is-a-key"))))
              #hasheq((isTest? . #t)))
(check-true ((λ () 
                 (put-object "test" "this-is-a-key" (hasheq 'isTest? #t))
                 (get-body (delete-object "test" "this-is-a-key")))))
(check-equal? ((λ () 
                 (put-object "test" "Hr05PhC5XRAtaWSGuBDCVU1T72c" 
                             "this is a test" (list "Content-Type: text/plain"))
                 (get-body (get-object "test" "Hr05PhC5XRAtaWSGuBDCVU1T72c"))))
              "this is a test")
(check-is-key? (get-body (post-object "test" (hasheq 'isTest? #t))))


;; ;;Need to put some objects in first... else its a 404
;; ;; (get-link "test" "doc3"   (list (hasheq 'bucket "test"
;; ;;                                                  'tag "_"
;; ;;                                                  'keep "1")
;; ;;                                          (hasheq 'bucket "_"
;; ;;                                                  'tag "next"
;; ;;                                                  'keep "1")))
;; (check-equal? (mapreduce
;;                (hasheq 'inputs "test"
;;                        'query (list
;;                                (hasheq 'link (hasheq 'bucket "test"))
;;                                (hasheq 'map (hasheq 'language "javascript"
;;                                                     'name "Riak.mapValuesJson"))))) '())
;; ;;Expect a fail until Riak 1.0
;; (check-exn exn:fail? (λ () (get-index "test" "this-is-a-key" "1"))) 
