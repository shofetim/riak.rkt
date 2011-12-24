#lang racket/base
(require rackunit
         "main.rkt")

(define-simple-check (check-is-hash? p)
  (hash? p))

(define-simple-check (check-is-key? p)
  (string? p))

(check-is-hash? (status)) ; Must have riak_kv_stat enabled for this
                          ; test, otherwise it is a 404
(check-equal? (ping) "OK")

;; (check-is-hash? (list-resources))
;; (check-is-hash? (list-buckets))
;; (check-is-hash? (list-keys "test"))
;; (check-is-hash? (get-bucket "test"))
;; (check-equal? (put-bucket "test" (hasheq 'props (hasheq 'n_val 5))) '())
;; (check-equal? (get-object "test" "Hr05PhC5XRAtaWSGuBDCVU1T72c") "\"this is a test\"")
;; (check-is-key? (post-object "test" (hasheq 'isTest? #t)))
;; (check-equal? ((λ () 
;;                    (put-object "test" "this-is-a-key" (hasheq 'isTest? #t))
;;                    (get-object "test" "this-is-a-key")))
;;               #hasheq((isTest? . #t)))
;; (check-true ((λ () 
;;                  (put-object "test" "this-is-a-key" (hasheq 'isTest? #t))
;;                  (delete-object "test" "this-is-a-key"))))
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
