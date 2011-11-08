#lang racket
;; API documentation http://wiki.basho.com/HTTP-API.html

(require net/url
         net/uri-codec
         )

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

(define (ping)
  (request "/ping"))



;; List Buckets
;; list-buckets: void -> json
;; not for production use (inefficient)
(define (list-buckets)
  (call/input-url
   (string->url (string-append server "/riak?buckets=true"))
   get-pure-port
   (lambda (in)
     (copy-port in (current-output-port))
     (newline))
   (list "User-Agent: racket")))

;; List Keys
;; list-keys: void -> json
;; not for production user
(define (list-keys bucket)
  (call/input-url
   (string->url (string-append server "/riak/" bucket "/?keys=true&props=false"))
   get-pure-port
   (lambda (in)
     (copy-port in (current-output-port))
     (newline))
   (list "User-Agent: racket")))

;; Bucket
;; bucket: void -> json
(define (bucket bucket)
  (call/input-url
   (string->url (string-append server "/riak/" bucket))
   get-pure-port
   (lambda (in)
     (copy-port in (current-output-port))
     (newline))
   (list "User-Agent: racket")))

;; Status
;; status: void -> json | string
(define (status [type "json"])
  (if (equal? type "json")
      (set! type "application/json")
      (set! type "text/plain"))
  (call/input-url
   (string->url (string-append server "/stats"))
   get-pure-port
   (lambda (in)
     (copy-port in (current-output-port))
     (newline))
   (list "User-Agent: racket"
         (string-append "Accept: " type))))

;; List Resources
;; list-resources: void -> json | string
(define (list-resources [type "json"])
  (if (equal? type "json")
      (set! type "application/json")
      (set! type "text/html"))
  (call/input-url
   (string->url (string-append server "/"))
   get-pure-port
   (lambda (in)
     (copy-port in (current-output-port))
     (newline))
   (list "User-Agent: racket"
         (string-append "Accept: " type))))

;; Get Bucket
;; get: string string -> mixed
(define (get bucket key)
  (call/input-url
   (string->url (string-append server "/riak/" bucket "/" key))
   get-pure-port
   (lambda (in)
     (copy-port in (current-output-port))
     (newline))
   (list "User-Agent: racket"
         "Accept: */*")))

;; Delete Bucket
;; delete: string string -> void
(define (delete bucket key)
  (call/input-url
   (string->url (string-append server "/riak/" bucket "/" key))
   delete-pure-port
   (lambda (in)
     (copy-port in (current-output-port))
     (newline))
   (list "User-Agent: racket")))

;; Post
;; not idempotent, racket supplies ket
;; post: string data -> void 
;; FIXME return the key instead
(define (post bucket data)
  (call/input-url
   (string->url (string-append server "/riak/" bucket))
   (lambda (url headers) (post-pure-port url (string->bytes/utf-8 data) headers))
   (lambda (in)
     (copy-port in (current-output-port)))
   (list "User-Agent: racket"
         "Content-Type: application/json")))

;; Put
;; idempotent
;; put: string string data -> void
(define (put bucket key data)
  (call/input-url
   (string->url (string-append server "/riak/" bucket "/" key))
   (lambda (url headers) (put-pure-port url (string->bytes/utf-8 data) headers))
   (lambda (in)
     (copy-port in (current-output-port)))
   (list "User-Agent: racket"
         "Content-Type: application/json")))

;; Put Bucket
;; idempotent
;; put-bucket: string json -> void
;; FIXME props needs to be encoded, json of some form?
(define (bucket-put bucket props)
  (call/input-url
   (string->url (string-append server "/riak/" bucket))
   (lambda (url headers) (put-pure-port url props headers))
   (lambda (in)
     (copy-port in (current-output-port)))
   (list "User-Agent: racket"
         "Content-Type: application/json")))

;; Map Reduce
;; map-reduce: json -> mixed
;; FIXME encode json
(define (map-reduce json)
  (call/input-url
   (string->url (string-append server "/mapred"))
   (lambda (url headers) (post-pure-port url json headers))
   (lambda (in)
     (copy-port in (current-output-port)))
   (list "User-Agent: racket"
         "Content-Type: application/json")))

;; Link Walk
;; ??
;; FIXME implementation
(define (link-walk bucket tag keep)
  "not yet implemented")

(define (request path #:debug [debug #f])
  (call/input-url
   (string->url (string-append server "/ping"))
   (if debug 
       get-impure-port
       get-pure-port)
   (lambda (in)
     (copy-port in (current-output-port))
     (newline))
   (list "User-Agent: racket"
          (string-append "X-Riak-ClientId: " client-id))))

(provide (all-defined-out))