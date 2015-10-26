# -*- coding: utf-8 -*-
import unittest
import mock
import parse_student_csv
import requests
from mock import call

#sample_csv =    "Grader,fname,mname,lname,suffix,otherinfo,address,address2,city,state,zip,nation,email,online,questions\r\n\
#                sb,Charles,anne ,Fisher,,,205 Country Club Dr .,,NATCHEZ,MS,39120,united sta,bible29@,No,learn more about the Bible\r\n\
#                sb,Janice,Opal,Osborne,,,1229 Camelia Avenue,,Kingsport,TN,37660,United St,jcoxjackie,No,Thank you for offering this BIBLE lesson to me\r\n"

class TestParseStudentCSV(unittest.TestCase):
 
    def setUp(self):
        self.zipMap = parse_student_csv.parse_zipcode_csv()
        pass

    def test_first_test(self):
        pass

    def test_parse_grader(self):
        json_output = parse_student_csv.produce_json_doc(filename="samples/newstudent.csv", zipmap=self.zipMap)
        self.assertEqual(len(json_output), 5)
        self.assertEqual(json_output[0]["Grader"], "sb")
        self.assertEqual(json_output[3]["Grader"], "dab")

    def test_parse_student(self):
        json_output = parse_student_csv.produce_json_doc(filename="samples/newstudent.csv", zipmap=self.zipMap)
        self.assertEqual(len(json_output), 5)
        self.assertEqual(json_output[0]["fname"], "Charles")
        self.assertEqual(json_output[0]["mname"], "anne")
        self.assertEqual(json_output[0]["lname"], "Fisher")
        self.assertEqual(json_output[0]["suffix"], "")
        self.assertEqual(json_output[4]["fname"], "Glen")
        self.assertEqual(json_output[4]["mname"], "Wilbur")
        self.assertEqual(json_output[4]["lname"], "Holland")
        self.assertEqual(json_output[4]["suffix"], "JR.")

    def test_parse_student(self):
        json_output = parse_student_csv.produce_json_doc(filename="samples/newstudent.csv", zipmap=self.zipMap)
        self.assertEqual(len(json_output), 5)
        self.assertEqual(json_output[0]["fname"], "John")
        self.assertEqual(json_output[0]["mname"], "bob")
        self.assertEqual(json_output[0]["lname"], "Doe1")
        self.assertEqual(json_output[0]["suffix"], "")
        self.assertEqual(json_output[4]["fname"], "John4")
        self.assertEqual(json_output[4]["mname"], "harry")
        self.assertEqual(json_output[4]["lname"], "Doe5")
        self.assertEqual(json_output[4]["suffix"], "JR.")

    def test_otherinfo(self):
        json_output = parse_student_csv.produce_json_doc(filename="samples/newstudent.csv", zipmap=self.zipMap)
        self.assertEqual(len(json_output), 5)
        self.assertEqual(json_output[3]["otherinfo"], "12345")
        self.assertEqual(json_output[4]["otherinfo"], "abcdefg")

    def test_address(self):
        json_output = parse_student_csv.produce_json_doc(filename="samples/newstudent.csv", zipmap=self.zipMap)
        self.assertEqual(len(json_output), 5)
        self.assertEqual(json_output[3]["address"], "794 John Doe Ave.")
        self.assertEqual(json_output[3]["city"], "Stuart")
        self.assertEqual(json_output[3]["state"], "VA")
        self.assertEqual(json_output[3]["zip"], "24171")
        self.assertEqual(json_output[3]["nation"], "usa")
        self.assertEqual(json_output[3]["location"]["lat"], 36.655575)
        self.assertEqual(json_output[3]["location"]["lon"], -80.23909)
        self.assertEqual(json_output[4]["address"], "1008 John Doe Ln Apt. # 801")
        self.assertEqual(json_output[4]["city"], "Bowling G")
        self.assertEqual(json_output[4]["state"], "OH")
        self.assertEqual(json_output[4]["zip"], "43402")
        self.assertEqual(json_output[4]["nation"], "United St")
        self.assertEqual(json_output[4]["location"]["lat"], 41.388519)
        self.assertEqual(json_output[4]["location"]["lon"], -83.65795)

    def test_email(self):
        json_output = parse_student_csv.produce_json_doc(filename="samples/newstudent.csv", zipmap=self.zipMap)
        self.assertEqual(len(json_output), 5)
        self.assertEqual(json_output[3]["email"], "doe4@")
        self.assertEqual(json_output[4]["email"], "doe5@")

    def test_online(self):
        json_output = parse_student_csv.produce_json_doc(filename="samples/newstudent.csv", zipmap=self.zipMap)
        self.assertEqual(len(json_output), 5)
        self.assertEqual(json_output[3]["online"], "No")
        self.assertEqual(json_output[4]["online"], "Yes")

    def test_timestamp(self):
        json_output = parse_student_csv.produce_json_doc(filename="samples/2014-02-05.csv", zipmap=self.zipMap)
        self.assertEqual(len(json_output), 5)
        self.assertEqual(json_output[3]["timestamp"], "2014-02-05T12:00:00")

    @mock.patch('requests.post')
    def test_send_to_elastic_search(self, mock_request):
        resp_mock = requests.Response()
        resp_mock.status_code = 200
        mock_request.side_effect = [resp_mock]
        data = list()
        data.append({"grader": "djb", "fname": "Bob"})
        data.append({"grader": "sb", "fname": "Fred"})
        parse_student_csv.send_to_elastic_search(data)
        exp_data = '{"index": {"_type": "new_student", "_index": "students"}}\r\n{"grader": "djb", "fname": "Bob"}\r\n{"index": {"_type": "new_student", "_index": "students"}}\r\n{"grader": "sb", "fname": "Fred"}\r\n'
        self.assertEqual(mock_request.mock_calls, [call(url='{}/_bulk'.format(parse_student_csv.ELK_STACK), 
                                            headers={'content-type': 'application/json'}, 
                                            data=exp_data)])

