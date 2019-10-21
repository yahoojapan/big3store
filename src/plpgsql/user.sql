/* -*- SQL -*-

   define user and database

   @copyright 2014-2018 UP FAMNIT and Yahoo Japan Corporation
   @version 0.3
   @since December, 2015
   @author Kiyoshi Nitta <knitta@yahoo-corp.jp>

   This file defines user of database for big3store system.

 */

CREATE ROLE b3s LOGIN SUPERUSER PASSWORD 'big3store';
CREATE DATABASE b3s OWNER b3s;

/* ====> END OF LINE <==== */
