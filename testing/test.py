from langchain_text_splitters import RecursiveCharacterTextSplitter

splitter = RecursiveCharacterTextSplitter(chunk_size=100,chunk_overlap=20)

test_text = """ Since  2001,  the  Government’s  data  security  policies  have  been  set  out  in  the  Government 
Instruction  Manual  (IM)  on  Infocomm  Technology  and  Smart  Systems  (ICT&SS)  Management.  \n\nIn 
2019,  the  Public  Sector  Data  Security  Review  Committee  recommended  additional  technical  and 
process  measures  to  protect  data  and  prevent  data  compromise.  The  recommended  measures 
have since been incorporated into the IM on ICT&SS Management.\n This document sets out the key policies in the IM on ICT&SS Management that govern how data 
security  is  managed  by  agencies.  The  policies  prescribe  data  security  requirements,  including 
technical and process measures, to safeguard data against security threats. \n The  technical  and  process  measures  described  in  this  document  are  implemented  using  a  risk-
based approach. \n\nAgencies should select the measures based on the data security risk level, after 
taking  into  account  their  operating  contexts.  For  example,  the  recommended  measure  “Hashing-
with-Salt”  irreversibly  changes  a  data  field  in  order  to  prevent  an  attacker  from  accessing  the 
original  value  of  the  data  field  even  when  the  data  is  extracted  from  the  system.  It  would  be 
suitable  for  use  in  analytics  systems  where  only  de-identified  data  is  required  but  not  appropriate 
for use in operational systems which require identifiable data for service delivery."""

chunks = splitter.split_text(test_text)

for chunk in chunks:
    print(chunk)
    print()
    print()
