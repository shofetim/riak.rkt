#lang racket
;; API documentation http://wiki.basho.com/HTTP-API.html

(require net/url
         net/uri-codec
         net/head
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
(define (get-object bucket key)
  (request "/riak" 'get (string-append "/" bucket "/" key)))

(define (put-object bucket key data [headers "Content-Type: application/json"])
  (request (string-append "/riak/" bucket "/" key) 'put data headers))

(define (post-object bucket data [headers "Content-Type: application/json"])
  (request (string-append "/riak/" bucket) 'post  data headers))

(define (delete-object bucket key)
  (request (string-append "/riak/" bucket "/" key) 'delete))

;;Link Walking
(define (get-link bucket key list-of-filters)
  ;;List of filters is a list of 3 element hashes, bucket, tag, keep
  (let ([data (format-filters list-of-filters)])
    (request (string-append "/riak/" bucket "/" key) 
             'get 
             data)))

(define (format-filters lst)
  (if (empty? lst)
      ""
      (let ([head (car lst)]
            [tail (rest lst)])
        (string-append "/" 
                       (hash-ref head 'bucket)
                       ","
                       (hash-ref head 'tag)
                       ","
                       (hash-ref head 'keep)
                       (format-filters tail)))))

;;Map Reduce
(define (mapreduce data)
  (request (string-append "/mapred") 'post  data "Content-Type: application/json"))

;;Secondary indexes

;;Luwak

;;Private
(define (request path [type 'get] [data ""] [headers ""])
  (cond
   [(eqv? 'get type)
    (call/input-url
     (string->url (string-append server path data))
     get-impure-port
     read-response
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
     read-response)]
   [(eqv? 'post type)
    (call/input-url
     (string->url (string-append server path))
     (λ (url)
        (post-impure-port url (string->bytes/utf-8 (jsexpr->json data))
                          (list "User-Agent: racket"
                                headers
                                (string-append "X-Riak-ClientId: " client-id))))
     read-response)]
   [(eqv? 'delete type)
    (call/input-url
     (string->url (string-append server path data))
     (λ (url)
        (delete-impure-port url
                            (list "User-Agent: racket"
                                  headers
                                  (string-append "X-Riak-ClientId: " client-id))))
     (λ (ip) 
        (read-response ip #t)))]
   [else (error "http method not implemented")]))

(define (read-response ip [is-delete #f])
  (let* ([return-headers (parse-headers ip)]
         [status (hash-ref return-headers "status")]
         [content-type (hash-ref return-headers "Content-Type")])
    (cond 
     ;;Handle HTTP erros
     [(and is-delete ;;a 404 is ok for a delete
           (or (= (string->number status) 404)
               (and
                (< (string->number status) 300)
                (> (string->number status) 199)))) #t]
     [(> (string->number status) 299)
      (error "HTTP errored with:" status)]
     ;;Choose a reader
     [(eof-object? (peek-char ip)) (check-headers return-headers)]
     [(equal? content-type "application/json") (read-json ip)]
     [(equal? content-type "text/html") (read-text/html ip)]
     [(equal? content-type "text/plain") (read-text/plain ip)]
     [else (error "No reader for content type:" content-type)])))

(define (parse-headers ip)
  (let* ([header-string (purify-port ip)]
         [status (substring header-string 9 12)]
         [headers (rest (regexp-split #rx"\r\n" header-string))]
         [alist-of-headers (map split-headers headers)])
    (make-hash (cons (cons "status" status) alist-of-headers))))

(define (split-headers str)
  (let ([tuple (regexp-split #rx":" str)])
    (if (equal? (car tuple) "")
        (cons '() '())
        (cons (car tuple) (remove-whitespace (cadr tuple))))))

(define (remove-whitespace str)
  (let ([1st (string-ref str 0)])
    (if (char-whitespace? 1st)
        (remove-whitespace (substring str 1))
        str)))

(define (check-headers ahash)
  (cond 
   [(hash-has-key? ahash "Location") (extract-key (hash-ref ahash "Location"))]
   [else '()]))

(define (extract-key str)
  (cadr (regexp-match #rx".*/(.*)$" str)))

(define (read-text/html ip)
  (read-text ip))

(define (read-text/plain ip)
  (read-text ip))

(define (read-text ip)
  (let ([out (open-output-string)])
    (for ([line (in-lines ip)])
         (write line out))
    (get-output-string out)))


(provide (all-defined-out))