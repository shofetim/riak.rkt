#lang racket
;; API documentation http://wiki.basho.com/HTTP-API.html

(require net/url
         net/uri-codec
         "json.rkt")

;; Settings
(define host "127.0.0.1")
(define port "8091")
(define protocol "http")
(define server (string-append protocol "://" host ":" port))
;; Default values for
;; r: Number of nodes that must respond to a read
;; w: Number of nodes that must respond to a write
;; dw: Number of nodes that must write to durable storage.
(define r 2)
(define w 2)
(define dw 2)
;; A unique client id for the vector clocks
(define client-id 
  (string-append 
   "rkt-" 
     (number->string (random 4294967087))))



;;;;; API
;; Server Operations
(define (ping)
  (request "/ping"))

(define (status [accept "Accept: application/json"])
  (request "/status" 'get "" accept))

(define (list-resources [accept "Accept: application/json"])
  (request "/"  'get "" accept))

;; Bucket Operations
(define (list-buckets) ;;EXPENSIVE!
  (request "/riak" 'get "?buckets=true" "Accept: application/json"))

(define (list-keys bucket) ;;EXPENSIVE!
  (request "/riak" 'get (string-append "/" bucket "?keys=true") "Accept: application/json"))

(define (get-bucket bucket)
  (request "/riak" 'get (string-append "/" bucket) "Accept: application/json"))

(define (put-bucket bucket props)
  (request (string-append "/riak/" bucket) 'put props "Content-Type: application/json"))

;; Object Operations










;;Private
(define (request path [type 'get] [data ""] [headers ""])
  (cond
   [(eqv? 'get type)
    (call/input-url
     (string->url (string-append server path data))
     get-impure-port
     (λ (ip)
        (check-status ip)
        (if (or (equal? path "/ping")
                (equal? path ""))
            (read (remove-headers ip))
            (read-json (remove-headers ip))))
     (list "User-Agent: racket"
           headers
           (string-append "X-Riak-ClientId: " client-id)))]
   [(eqv? 'put type)
    (call/input-url
     (string->url (string-append server path))
     (λ (url)
        (put-impure-port url 
                         (string->bytes/utf-8 (jsexpr->json data))
                         (list "User-Agent: racket"
                               headers
                               (string-append "X-Riak-ClientId: " client-id))))
     (λ (ip)
        (check-status ip)
        (if (eof-object? (peek-char ip))
            '()
            (read-json (remove-headers ip)))))]
   [(eqv? 'post type)
    (call/input-url
     (string->url (string-append server path))
     (λ (url)
           (post-pure-port url (string->bytes/utf-8 (jsexpr->json data))))
     (λ (ip)
           (read-json ip)))]
   [(eqv? 'delete type)
    (call/input-url
     (string->url (string-append server path data))
     delete-pure-port
     (λ (ip)
           (read-json ip)))]
   [else (error "http method not implemented")]))

(define (check-status ip)
  (let* ([headers (purify-port ip)]
         [status (substring headers 9 12)])
    (when (not (or (equal? status "200")
                   (equal? status "204")))
      (error "Error server returned:" (string->number status)))))

(define (remove-headers ip)
  (let* ([headers (purify-port ip)]
        [header-length (string-length headers)])
    (for ([i (in-range 0 header-length)])
         (read-char ip))
    ip))

(provide (all-defined-out))