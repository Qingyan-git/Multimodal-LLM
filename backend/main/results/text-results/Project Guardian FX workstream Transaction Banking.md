
01/07/2025 

1 

--- end of page.page_number=1 ---

## **Disclaimer** 

This report focuses on various design, operational and risk considerations in the use of tokenised bank liabilities and shared ledger in cross-border payments.  The terminology of tokenised bank liabilities is used in this paper in a broad, functional sense to refer to tokenised representation of commercial bank money on a shared ledger, without asserting any specific legal or regulatory interpretation. This paper does not set out any positions on the legal classification or regulatory treatment of any tokenised bank liabilities which may vary by jurisdictions. 

This report and its contents are made available on an “as-is” basis without warranties of any kind. The content in this report does not constitute regulatory, financial, legal or any other professional advice and should not be acted on as such. None of its authors and contributors shall be liable for any damage or loss of any kind howsoever caused as a result from the use of the information contained or referenced in this report. 

This report does not seek to address policy objectives or recommend any specific solution or product. Whilst the content strives to provide more clarity on the subject matter, the authors of this report make no representation or guarantees on the performance or adequacy of the solutions or models. The examples used in the report are only for illustration purposes. 

The views and recommendations expressed are those by the lead contributors and do not imply a consensus view by market stakeholders across the spectrum of international debt capital markets. 

## **Document Version** 

||Version|Date|Author|Rationale|
|---|---|---|---|---|
||1.0|July 2025|Guardian Foreign<br>Exchange Industry<br>Group|First publication|
||||||
||01/07/2025||||
|||2|||



--- end of page.page_number=2 ---

## **Acknowledgement** 

This is a joint report developed by the Guardian Foreign Exchange Industry Group. The report was led by Ant International, in collaboration with the International Swaps and Derivatives Association (ISDA). 

The report received additional contributions from organisations listed alphabetically: 

- Global Financial Markets Association’s Global Foreign Exchange Division (GFXD) 

- Monetary Authority of Singapore (MAS) 

- OCBC 

- The Bank of New York Mellon 

- The Hongkong and Shanghai Banking Corporation Limited (HSBC) 

01/07/2025 

3 

--- end of page.page_number=3 ---

## Table of Contents 

|**1**|**Introduction**...............................................................................................................................**5**|
|---|---|
|**2**|**Background**................................................................................................................................**7**|
|2.1|Cross-border payments and the FX market ........................................................................ 7|
|2.2|Differences between traditional transactions and transactions using tokenised bank|
|liabilities  ........................................................................................................................................... 7||
|**3**|**Implementation of shared ledger and tokenised bank liabilities in transaction banking**....**10**|
|3.1|Key lifecycle stages of traditional cross-border payments ............................................... 10|
|3.2|Design Considerations ...................................................................................................... 15|
|**4**|**Operating models and considerations**....................................................................................**19**|
|4.1|Changes in operating practices ......................................................................................... 19|
|4.2|Potential operating models .............................................................................................. 23|
|**5**|**Risk considerations and mitigants**..........................................................................................**24**|
|5.1|Risk Considerations ........................................................................................................... 24|
|5.2|Risk Mitigants .................................................................................................................... 26|
|**6**|**Case studies and examples**.....................................................................................................**28**|
|6.1|Use-case 1: Ant International  .......................................................................................... 28|
|6.2|Use-case 2: BNY and OCBC ............................................................................................... 31|
|6.3|Use-case 3: HSBC .............................................................................................................. 33|
|**7**|**Developing standardised documentation for tokenised FX transactions**.............................**36**|
|7.1|Existing industry standards ............................................................................................... 36|
|7.2|Ongoing industry initiatives and regulatory developments ............................................. 37|
|**8**|**Conclusion**...............................................................................................................................**41**|
|**9**|**References**...............................................................................................................................**43**|



01/07/2025 

4 

--- end of page.page_number=4 ---

## **1 Introduction** 

For the purposes of this paper, the terms ‘shared ledger’ and 'shared ledger infrastructure' refer to a common foundational infrastructure, such as a Distributed Ledger Technology (DLT) or blockchain network, where data is replicated, synchronised and shared across a network of participants. 

Transaction banking refers to the provision of instruments and services by a financial institution to corporate and institutional clients, enabling their day-to-day needs through payment processing (e.g., domestic and cross-border payments), cash management, trade finance, and securities services. 

In today’s global economy, participants in cross-border transactions are exposed to foreign exchange (“FX”) risks. FX is frequently used by corporates to settle payments in foreign currency or hedge exposure to currency risk where corporates realise income and pay expenses in different currencies. Managing these risks effectively is essential for ensuring financial health and meeting business goals. 

Traditional FX solutions are often bound either by market opening hours or requiring alternative arrangements which typically come with additional fees. Many systems still rely on legacy systems, including Real Time Gross Settlement (RTGS) and correspondent banking flows requiring downtime for maintenance. There have been strides made in traditional banking including the extension of trading hours and enhancements to RTGS networks, which, have helped reduce risk and enhance efficiency. Initiatives by bodies such as the FSB and CPMI aim to align RTGS operating hours and enhance infrastructure effectiveness.[1,2] Additionally, corporates frequently encounter challenges in understanding the full end-toend cost of executing cross-border payments. 

## _**Potential use of tokenised bank liabilities**_ 

Tokenisation and shared ledger infrastructure have great potential to alleviate some of the pain points faced by market participants. Tokenisation refers to the process of converting a real-world asset, such as a traditional deposit, into a digital representation or "token" on a shared ledger. These digital tokens can act as secure, verifiable representations of the original asset. They can be transacted, transferred, or stored in a way that matches the functionality of traditional banking products, with enhanced efficiency, transparency, and security. 

Among the most relevant innovations enabled by tokenisation is the emergence of digital money (tokenised money) formats such as tokenised bank liabilities, stablecoins and central bank digital currencies (CBDCs)[ 3] , which are designed to function as stable store of value. 

> 1    https://www.fsb.org/wp-content/uploads/P211024-1.pdf 2    https://www.bis.org/cpmi/publ/d203.pdf 

> 3  Central bank digital currencies (CBDCs) and stablecoins are other forms of digital money with differences in overall design and governance in comparison to tokenised bank liabilities. CBDC is a direct liability of the issuing central bank, while private entities can issue stablecoins and are designed to maintain a constant value against one or more specified fiat currencies. On top of tokenised bank liabilities, the industry must carefully monitor the evolving landscape of stablecoins and CBDCs to navigate potential opportunities and challenges effectively. 

01/07/2025 

5 

--- end of page.page_number=5 ---

This stability is critical for payments and settlement, where parity and redeemability into fiat currency remains essential during the transition towards broader recognition and acceptance of digital money. In the context of tokenised bank liabilities[4] used in transaction banking, tokenisation enables bank liabilities to be held and transferred as digital tokens, facilitating faster settlement and improving the overall accessibility of financial transactions. 

This paper will provide an overview of the potential applications of shared ledgers and tokenised bank liabilities used in transaction banking. In this respect, a common feature amongst many initiatives is a focus on use of the tokenisation of money and “smart contracts” to streamline the frequently complex process of FX payments and settlements.[5] 

Whilst recognising the development of various forms of digital money, including CBDCs and stablecoins, this paper will focus on the use of tokenised bank liabilities to facilitate payments and settlement in transaction banking. 

In this vein, this paper will: 

- focus on two pain points identified by workstream participants, namely: (i) implementing tokenised bank liabilities and shared ledger solutions in cross-border payments and FX settlements; and (ii) the lack of a generally accepted industrywide framework facilitating the adoption of tokenised bank liabilities; and 

- showcase Project Guardian use cases which have been designed to consider and/or address the pain points. In particular, these uses cases illustrate solutions that enable tokenised deposits in different currencies and issued by different deposit takers to be exchanged. While the use cases are in the experimental phase, these efforts nevertheless demonstrate that scalable and interoperable solutions can be developed in anticipation of tokenised assets market growth. 

This paper also expands on various design, operational and risk considerations in the use of tokenised bank liabilities and shared ledger in cross-border payments.  This paper does not set out any positions on the legal classification or regulatory treatment of any tokenised bank liabilities which may vary by jurisdictions. 

> 4 Tokenised bank liabilities are tokens on a ledger that represent a commercial bank's liability, potentially including other forms of bank liabilities beyond deposits. 

> 5 This is one aspect discussed by the Bank for International Settlements (“ **BIS** ”) in _Blueprint for the future monetary system: improving the old, enabling the new_ https://www.bis.org/publ/arpdf/ar2023e3.htm. This paper focusses on private sector initiatives as opposed to the public sector, emphasising on tokenised deposits rather than initiatives which involve the input of central banks or governments, for example, central bank digital currencies (CBDCs). 

01/07/2025 

6 

--- end of page.page_number=6 ---

## **2 Background** 

## **2.1 Cross-border payments and the FX market** 

Liquidity management is a cornerstone of a treasury function facilitated by transaction banking solutions, requiring the seamless movement of currencies to meet payment obligations and funding needs. 

Effective liquidity management facilitates domestic and cross-border payment obligations, the optimisation of cash flows, and, in turn, improves financial stability. Liquidity management often involves converting currencies and managing intra-day or short-term funding gaps to avoid disruptions in payments or settlements and prevention of a wider systemic risk. 

In times of market volatility, domestic and cross-border payments paired with FX transactions enabling liquidity solutions becomes even more critical, as they allow institutions to respond swiftly to changing conditions and maintain confidence in their operations. These activities not only address immediate operational needs but also contribute to the depth and resilience of the global FX market, thereby also maintaining price continuity and price stability in the FX markets. 

## **2.2 Differences between traditional transactions and transactions using tokenised bank liabilities** 

The BIS has carried out extensive research on the use cases where tokenisation is easiest and where systemic gains are expected to be greatest.[6] The BIS notes that tokenisation can “dramatically enhance the capabilities of the monetary and financial system by harnessing new ways for intermediaries to interact in serving end users, removing the traditional separation of messaging, reconciliation and settlement”. 

In this respect, tokenised bank liabilities have the potential to alleviate some key pain points for transaction banking: 

_**Complexity of cross-border payments:**_ Payments in the transaction banking space are frequently cross-border, involving different currencies and time zones. Cross-border settlement requires different intermediaries and disparate payment systems across institutions and can be expensive, slow and opaque, reflecting multiple frictions.[7] Tokenised bank liabilities, as a programmable layer of money, have the potential to simplify crossborder payment processes by reducing reliance on intermediaries and enabling settlement over shared infrastructure/system. 

_**Dependency on multiple local RTGS clearing bound by cut-off times:**_ FX markets are only open five and a half days a week. The payment of these transactions is affected by time differences and payment systems operating hours. Meanwhile, shared ledger and tokenised 

> 6     https://www.bis.org/publ/arpdf/ar2023e3.htm 

> 7 - Financial Stability Board (FSB). 2020. “Enhancing Cross-Border Payments: Stage 3 Roadmap.” Basel, https://www.fsb.org/wp - content/uploads/P131020 1.pdf; Committee on Payments and Market Infrastructure (CPMI). 2020. “Enhancing Cross-Border Payments: Building Blocks of a Global Roadmap,” Stage 2 report to the G20. Bank for International Settlements, Basel. https://www.bis.org/cpmi/publ/d193.pdf. 

01/07/2025 

7 

--- end of page.page_number=7 ---

bank liabilities-based solutions offer 24/7 operations. Payment systems who use these solutions could potentially eliminate delays caused by varying cut-off times due to time difference, and non-operating markets on weekends and public holidays. 

_**Consequent Implications for FX settlement:**_ The complexity of cross-border payments and dependency on RTGS clearing consequently create downstream implications in the settlement of FX transactions. Two notable implications are the lack of instantaneous settlement (with standard market conventions being T+1/T+2 for most pairs) and the presence of settlement risks, with shared ledger being well-positioned to address the first implication as it is potentially available 24/7. On the second implication, CLS Group’s product, CLSSettlement provides an effective risk mitigation and multilateral netting solution.[8] However, only payments in 18 currencies[9] are currently supported. A recent estimate[10] suggested that 10%-15% of trades by value are settled on a gross bilateral basis without risk mitigation which is likely due to either the currency or counterparty not being CLS eligible. Shared ledger and tokenised bank liabilities coupled with adequate operational and legal frameworks have the potential to mitigate this risk, enabling atomic settlement for a wide range of currencies with commonly recognised settlement finality. 

_**Fragmented settlement layers:**_ Operational friction arises from distinct settlement layers, each managed by different parties with independent workflows. Implementing shared ledger can simplify this by integrating settlement into a single process and technology layer, reducing intermediary reliance, and enhancing transaction visibility in the long run. That said, meaningful implementation will require a sufficient number of participants to commit to the development and adoption of interoperable solutions that are consistent with their internal governance, compliance, and control requirements. 

- _**Cost of cross border payment**_ : In cross-border payments, a common settlement asset or common settlement platform does not exist. The use of multiple intermediaries in traditional cross-border payment adds to transaction costs, notably for retail payments, despite G20 efforts to reduce them[11] . The lack of a common settlement asset requires banks to either extend credit to each other or to pre-fund potential cross-border payment needs. The funds locked in these pre-funding accounts represent opportunity costs, which may ultimately be passed on in the form of fees. Interoperable tokenised bank liabilities used to complete settlement may reduce the need for multiple intermediaries and the associated costs. There are also potential indirect savings from improved operational efficiencies through the deployment of smart contracts, though it should be noted that it may be offset by the cost of initial setup and implementation. 

> 8  https://www.cls-group.com/products/settlement/clssettlement/ 

> 9 At the time of this paper, CLS supports 18 currencies and settles 94% of average daily FX volumes on a PvP basis. 

> 10  https://www.bankofengland.co.uk/speech/2024/december/philippe-lintern-speech-at-fx-markets-europe-on-global-fx-code 

> 11 The FSB has studied the cost of cross-border payments. For retail cross-border payments, the FSB’s target is to bring the global average cost of payment to be no more than 1%, with no payment corridors with costs higher than 3% by end-2027. The FSB notes that the average costs for B2B cross-border payment transactions in 2024 is still 1.6%; the average cost of B2P and P2B cross-border payment transactions is 2% and that for P2P transactions is higher at 2.6%. FSB _Annual Progress Report on Meeting the Targets for Cross-border Payments_ , _2024 Report on Key Performance Indicators_ https://www.fsb.org/uploads/P211024-3.pdf 

01/07/2025 

8 

--- end of page.page_number=8 ---

_**Capital controls affecting movement of domestic currency:**_ Select countries implement currency-based Capital Flow Management Measures (e.g., CFMM) to manage and regulate financial flows. The manner in which capital controls are currently implemented can affect the access to funds and efficiency of cross-border settlement when involving affected currencies. Tokenisation allows for the coupling of programmability with value transfer (in the form of digital money). This can enable the embedding of capital control policies (e.g., CFMM as noted in BISIH's Project Mandala or CNY/CNH rate curves) in the transfer of digital money for cross-border payments, promoting greater efficiency, transparency and potentially greater compliance adherence. 

01/07/2025 

9 

--- end of page.page_number=9 ---

## **3 Implementation of shared ledger and tokenised bank liabilities in transaction banking** 

## **3.1 Key lifecycle stages of traditional cross-border payments** 

This section explores how tokenised bank liabilities can enhance workflow efficiency and interoperability compared to the existing operating model. While real-world implementation involves significant complexity and dependencies, this high-level perspective provides a foundational benchmark for assessing the evolution of payments. 

Additionally, we explore the key building blocks within operating models that support gradual integration, ensuring that tokenised bank liabilities can effectively function in transaction banking. 

## **3.1.1 Traditional Lifecycle of Cross-Border Payments** 

**==> picture [403 x 175] intentionally omitted <==**

_Figure 1: Illustration of a traditional lifecycle of cross-border payments_ 

The figure above illustrates a simplified traditional lifecycle of cross-border cross-currency payments, facilitated by a correspondent bank. For instance, Corporate A is making a payment to Corporate B, who is in country B and receiving in another currency (i.e., Currency B). Such cross-border cross-currency payments are common in international trade, manufacturing and supply chain management, cross-border asset management, inter-subsidiary/group payments, etc. 

In this case, 

1. First, Corporate A (i.e., Sender) initiates a payment request and FX transaction with its bank (e.g., Bank A) by providing details of the recipient (e.g., bank account and purpose of payment). 

2. Bank A (i.e., Sender’s Bank) receives the payment request and validates the payment order ensuring that there are sufficient funds and required documentation prior to sending SWIFT message (MT103) to Bank B (i.e., the Receiver’s Bank). Bank A also conducts the necessary Anti-Money Laundering (AML) and Countering Financing of Terrorism (CFT) checks. 

01/07/2025 

10 

--- end of page.page_number=10 ---

3. In the case where there is no correspondent banking relationship between Bank A and Bank B, an intermediary bank may be engaged. The intermediary bank may be servicing the FX for this transaction. In this case, the intermediary bank would receive a SWIFT message (e.g., MT202 or MT202 COV) to facilitate funds movement between banks. The intermediary bank would similarly check on compliance, ensures liquidity and processes the funds through its own channels (e.g., nostro/vostro with Bank A and Bank B accordingly). 

4. Bank B (i.e., Receiving bank) receives the SWIFT message (e.g., MT103 message) and verifies the payment details (e.g., Corporate B’s account details, amount to be received). Bank B also conducts the necessary AML and CFT checks as the receiving bank. 

5. Payment is processed by Bank B and credited to Corporate B's account. Corporate B is notified that funds are received and can be deployed to other use. 

The funds are generally settled across the banks through correspondent banking between banks, or independent multi-currency settlement systems: 

1. With correspondent banking arrangement, the end-to-end payment and settlement time may increase while transparency would be reduced when the payment chain increases in complexity and see greater number of intermediary bank involvement. Further, the cut-off times of RTGS systems and time difference across time zones would affect the time required for settlement after the initial payment initiation. 

2. Independent multi-currency settlement systems are infrastructures with settlement capabilities in several currencies[12] . These systems allow for payments versus payments (PvP), addressing settlement risk in cross-currency payments. However, some of these systems may only support a specific set of currencies, and costs and operational complexity[13] have been cited as other potential challenges to adopt such systems. 

## **3.1.2 Innovating Cross-Border Payments with Tokenised Bank Liabilities** 

Tokenised bank liabilities can potentially facilitate more efficient transaction banking services, which could be faster, cheaper and have greater transparency. There are different approaches to which tokenised bank liabilities may be implemented at the token level and ledger level. 

_**1. Token Level: Tokenised bank liabilities could take the form of a digital twin tokens or a digitally native token.**_ 

   - a) _Digital twin tokens_ 

> 12  Bech, M, U Faruqui and T Shirakami (2020): “Payments without borders”, BIS Quarterly Review, March, www.bis.org/publ/qtrpdf/r_qt2003h.pdf 

> 13  Settlement risk in foreign exchange markets and CLS Bank. BIS Quarterly Review, December 2002 https://www.bis.org/publ/qtrpdf/r_qt0212f.pdf 

01/07/2025 

11 

--- end of page.page_number=11 ---

A form of tokenised asset that is issued and custodied traditionally, but also converted onto a shared ledger network through digital twin tokens that convey ownership interests in the underlying traditional assets (i.e., a bank liability), representing a claim against the issuing institutions. The tokenisation process involves converting ownership and rights of the traditional bank liability into a digital token that can be more easily and efficiently transferred, settled and managed over shared ledger-based systems. Banks’ tokenised bank liabilities is an example of digital twin tokens where the depository institution that issued the token is liable to the owner of the underlying off-chain liability, with the transferrable ownership rights represented on-chain by the token[14] . In the transaction banking context, the nature of this form of digital tokens makes it a suitable medium for payments and settlement. 

## b) _Digital native tokens_ 

A reference to assets that are issued and custodied natively on a shared ledger only (which constitutes the golden source of truth in relation to ownership rights), and therefore do not have traditional assets as an underlying basis[15] . Digital native tokens are legally recognised as existing on-chain only, with shared ledger networks serving as official asset registers, and functionality encoded via smart contracts allowing for automated and transparent behaviour. 

_**2. Ledger Level: Tokenised bank liabilities’ transactions on a shared ledger can be executed in different ways: within a single bank’s group ledger, between two banks’ ledgers or across interlinking distinct networks.**_ 

## _a) Within a single bank[16] ledger_ 

A single bank may establish a shared ledger with its entities or branches hosting the nodes. Participants may join this common shared ledger and transact bilaterally or with multiple counterparties within this network, with transactions being atomically settled on a delivery-versus-payment (DvP) or payment-versus-payment (PvP). In such an implementation, corporates can leverage tokenised bank liabilities to optimise their liquidity management by freely moving multi-currency deposits across their subsidiaries in multiple jurisdictions. Tokenised bank liabilities are designed to enable real time, 24/7 fund transfers and deployment in a single bank’s group ledger allow cross-border payments to be processed as “on-us” transactions. While these are existing capabilities in traditional banking today, a single bank ledger remains a crucial foundational piece for various deployment models involving tokenised bank liabilities and potentially interoperability with other forms of digital money. This design also generally offers banks and corporates comfort in terms of risk management, security and safety implication. 

> 14  Project Guardian Open interoperable network, https://www.mas.gov.sg/publications/monographs-or-informationpaper/2023/project-guardian-open-interoperable-networks 

> 15  Ibid. 

> 16   Single bank ledger in this context, refers to an internal shared ledger operated within the same banking group, enabling intra-group transfers between accounts held across different legal entities or branches. Such transfers can be effected cross-border, subject to the geographic and regulatory coverage of the banking group. 

01/07/2025 

12 

--- end of page.page_number=12 ---

Furthermore, the digital nature of tokenised bank liabilities enables programmability, allowing different conditions (e.g., rule-based target balances and capital flow measures) to be embedded. This introduces greater flexibility and customisation for corporates in liquidity management while also potentially incorporating compliance with jurisdiction-specific policies. For instance, corporates can determine the target balances across various entities in different jurisdictions across various currencies. Corporates need not follow fixed rules or fields in determining target balances and specific cut-off times. 

## _b) Between banks_ 

Due to the nature of international trade, corporates may need to make payments to suppliers/merchants that bank with different banks. To facilitate such payments, banks may choose to leverage their respective tokenised bank liabilities across their own shared ledger networks. This would serve as an alternative to existing correspondent banking. From the corporates’ perspective, tokenised bank liabilities deployed across banks’ shared ledger can similarly be executed on a 24/7 basis, and the ability to support PvP, combined with the atomic nature of the transactions, enhances settlement certainty and mitigates settlement risk. 

PvP could be orchestrated through varying technical implementations such as escrow smart contracts, cross-chain swaps or protocols. While it may be technically feasible, this approach will require further exploration. Depending on the implementation, it may require financial institutions to be comfortable holding the counterparty’s tokenised bank liabilities on the counterparty’s ledger and there may be an element of prefunding if its implementation emulates existing correspondent banking models. 

Prior to PvP execution between two different banks’ shared ledger, it has been observed that banks may leverage cross-chain implementation for the exchanges of messages (e.g., through cross-chain mechanism such as Hashed Timelock Contracts (HTLC)) as an interim to better understand the risks, considerations and controls required for cross-chain interactions for value transfer. 

## _c) Across interlinking distinct networks[17]_ 

There are multiple efforts internationally exploring a network of interconnected banks transacting with tokenised monies. These interlinked networks can consist of a network of independent or layered networks, application-specific chains, or sidechains, each with their own distinct governance framework and other customisations to facilitate clearing and settlement of one or more currencies. Depending on the design and the governance of the network(s), banks can deploy tokenised (cash and non-cash) assets and facilitate transactions on a PvP and/or 

> 17  Project Guardian Interlinking Networks Technical Paper (2023), https://www.mas.gov.sg/publications/monographs-or-informationpaper/2023/interlinking-networks 

01/07/2025 

13 

--- end of page.page_number=13 ---

DvP basis. As corporates generally diversify their risks and do not rely on a single bank, such network(s) could serve as an international settlement network that better facilitates multi-bank transactions and addresses liquidity fragmentation. 

Cross-border payments using tokenised bank liabilities can be implemented through various models, which generally fall into a few broad categories. The table below summarises these potential implementation models. 

|Implementation<br>Model|Interoperability<br>protocol required|Currency<br>conversion required|Key Considerations|
|---|---|---|---|
|Single Bank Shared<br>Ledger Network<br>(Single Currency)|No|No|Operates within a<br>single bank’s shared<br>ledger to enable<br>internal real-time<br>transfers.|
|Multi-Bank (Single<br>Currency, Same<br>Shared Ledger<br>Network)|Partial18|No|Participating banks<br>operate on a<br>shared<br>permissioned<br>ledger with needed<br>agreement on<br>access, rules, and<br>settlement.|
|Multi-Bank (Single<br>Currency, Different<br>Shared Ledger<br>Networks)|Yes|No|Participating banks<br>operate on<br>separate ledgers.<br>Interoperability<br>protocols would be<br>required to<br>coordinate<br>messaging and<br>settlement.|
|Cross-Currency<br>(Single or Multi-<br>bank)|Depending on the<br>Shared Ledger<br>network and<br>tokenised deposit<br>pair involved.|Yes|FX mechanism is<br>required to support<br>cross-currency<br>transactions,<br>operating within a<br>single shared ledger<br>network or across<br>networks.|



_Table 1: Potential implementation models for cross-border payments using tokenised bank liabilities_ 

> 18  In a scenario where a bank adopts another bank’s shared ledger network, some integration and connectivity may be required to ensure alignment with the host network’s governance and technical standards. 

01/07/2025 

14 

--- end of page.page_number=14 ---

## **3.2 Design Considerations** 

## **3.2.1 Building towards interoperable tokenised bank liabilities transactions** 

A real-world application of cross-border payments with tokenised bank liabilities would likely face a scenario where the tokenised bank liabilities are denominated in different currencies and issued on different shared ledger networks by different banks. This raises the question as to how to ensure interoperability of tokens across different shared ledgers. 

Though this is analogous with the traditional model where there are dependencies on correspondent banking relationships or the involvement of intermediary banks, designing operating models on the shared ledger network allows for an opportunity to reimagine the messaging, clearing and settlement processes between financial institutions. 

Interoperability remains a fundamental prerequisite for the wider adoption of tokenised bank liabilities-based transactions and continues to be a focus area for financial institutions and market participants’ resources. 

In one approach, further explored in this paper under Use Case 1, a liquidity provider is in effect the new intermediary and swaps tokens from one currency to another. The paying bank only needs to participate on one shared ledger network and interface with the liquidity provider to connect to other shared ledger networks and complete the payment. Standardised messaging on both shared ledger networks will be needed to ensure all payment requests capture the same information and can be completed. 

In another approach showcased in Use Case 2, payments performed on two private and permissioned shared ledger networks explored using Hash Time-Locked Contracts to establish a bilateral exchange of either messages or tokens across the two chains. This method is able to streamline both banks’ operations and reliance on a single coordination point. 

## **3.2.2 Price quotations for cross-currency transactions** 

Another key consideration in the interoperability of different currency tokenised bank liabilities would be the sourcing of price quotations for cross-currency transactions, both within the same financial institution’s shared ledger network and between financial institutions. 

To provide timely and accurate price quotations for FX conversion rates, network operators should consider how established market best practices can be incorporated into the shared ledger ecosystem. 

In the early stages of a shared ledger network, on-chain trading volumes, when implemented, will not be sufficient to generate reliable market FX rates purely from onchain data. Therefore, an automated method, such as a price oracle, will be needed to source and incorporate reliable market FX data with minimal latency. 

01/07/2025 

15 

--- end of page.page_number=15 ---

To ensure a robust and representative data sample while minimising the likelihood of outliers, it is recommended to aggregate multiple institutional grade data sources using algorithmic computations. Potential sources of live market data could include central bank benchmark rates, internal bank liquidity desks, and market data providers like Bloomberg and Reuters. Due to the computationally intensive nature of these algorithms, it may be more efficient to execute the calculations off-chain. Once the data sources have been aggregated into a single benchmark rate for each currency pair, they can then be integrated into the shared ledger network via an oracle service. The oracle can be fed by APIs at regular high frequency intervals to ensure secure and efficient data transmission. 

Maintaining data integrity and preventing any type of manipulation should be a top priority to ensure confidence in the network. Data fed to the shared ledger network should be updated with minimum viable time latency as prices are updated on existing electronic platforms every two microseconds and incorporate functionality to check for ‘stale’ prices. Anti-manipulation measures could be implemented via cryptographic tools like basic hash functions to prove that the data has not been tampered with without revealing any sensitive information. 

At some point in the future, the shared ledger ecosystem will have developed to a point where on-chain price formation and execution are possible such that there will be less reliance on existing venues used for discovery. The on-chain FX data can be made visible to all users in a way that obfuscates private data of individual users and transactions while using advanced cryptographic tools such as zero-knowledge proofs to demonstrate the data’s validity. The aggregated public market data will be available simultaneously to all users and could prevent rent-seeking behaviour that can result from information asymmetries. 

## **3.2.3 Design principles for tokenised bank liabilities in transaction banking** 

To promote interoperability across different product frameworks and operating models for tokenised assets, a set of standardised design principles for tokenised fixed income products was developed under the Guardian Fixed Income Framework.[19] The design principles are technology and jurisdiction-agnostic. Some of these design principles also apply to tokenised bank liabilities and relate to smart contracts insofar as they are used to create the tokens to which tokenised bank liabilities relate. These are as summarised below: 

|**#**|**Principle**|**Description**|
|---|---|---|
|1|Valid existence of<br>the issuer|At the time of issuance and during the life of the tokenised<br>bank liabilities, the issuer of the tokenised bank liabilities,<br>which is typically a financial institution, is duly established and<br>validly existing under the law under which it is organised.|



> 19 Project Guardian: Fixed income Framework, November 2024 

01/07/2025 

16 

--- end of page.page_number=16 ---

|**#**|**Principle**|**Description**|
|---|---|---|
|2|Validity<br>of<br>the<br>tokenised<br>bank<br>liabilities|The tokenised bank liabilities are duly issued and constitute<br>legal, valid and binding obligations of the issuer. The tokenised<br>bank liabilities and the related legal rights should be capable of<br>being attached with, or themselves constitute a digital token<br>recorded on a ledger20, consistent with the governing law of<br>the jurisdiction in which the tokenised bank liabilities are<br>recognised. In the case of digital twin tokens, the tokenised<br>bank liabilities and the underlying legal rights are inextricably<br>linked – meaning that the token cannot be transferred<br>independently of the legal rights and vice versa.21|
|3|Authorisations<br>and consents|The issuer and any intermediaries or service providers should<br>have obtained and will maintain all relevant authorisations and<br>consents, including any licenses, from any relevant supervisory<br>or regulatory authority.|
|4|Tokenisation<br>process|The process of tokenisation, including the use of shared ledger<br>and smart contracts, and the maintenance of any records<br>relating to the tokenised bank liabilities, should not alter or<br>affect the terms of the transaction documentation. If any<br>inconsistency arises, appropriate measures should be taken to<br>ensure continued alignment with the agreed legal terms and<br>compliance with all applicable laws. Any restrictions on<br>transferability should be embedded in the functionalities of<br>the smart contract or be compatible with the technology<br>platform.|
|5|The ledger|The applicable ledger should comply with any applicable law<br>and have regard to any applicable principles, standards and<br>best practices developed and recognised by industry bodies,<br>trade associations, or as commonly adopted in the market. The<br>ledger should be able to accommodate disposals and transfers<br>of the tokenised bank liabilities and be public and transparent.<br>The ledger should have integrity and be fit for purpose.|
|6|Minimum<br>features of smart<br>contracts|The minimum features of smart contracts should address the<br>governance of the smart contract and should use technology<br>and risk frameworks to mitigate smart contract risks.|
|7|Tokenisation<br>terms|The transaction documentation should take into account and<br>be consistent with the governing law of the tokenised bank<br>liabilities. It should clearly set out the rights of holder and|



> 20  These design principles use the generic term "ledger" to refer to ledgers built using shared ledger, whether such ledgers are centralised or decentralised and permissioned or permissionless. The term ledger should be read accordingly. 

> 21  Whether such linkage is enforceable will depend on the legal recognition and treatment of tokenised representations in the relevant jurisdiction. 

01/07/2025 

17 

--- end of page.page_number=17 ---

|**#**|**Principle**|**Description**|
|---|---|---|
|||disclose any associated risks. The transaction documentation<br>should establish a transparent and fair process for addressing<br>loss or theft of a private key and managing the consequences<br>of token cancellation. There should be clearly defined<br>procedures for detecting and responding to any breaks in<br>reconciliation, including the investigation measures and<br>related communication. At the same time, the documentation<br>should reflect the diversity of tokenised products and variance<br>in their legal and technical features.|
|8|Information<br>on<br>the functioning of<br>the ledger and<br>smart contract|The issuer should make available to that each holder of the<br>tokenised bank liabilities all material information regarding the<br>functioning of the ledger and the smart contract used for the<br>tokenisation process. This includes clear and accessible<br>information on the technical and organisational measures<br>implemented to protect the functioning, integrity and security<br>of the ledger and the smart contract.|
|9|Risk management|There should be a rigorous governance framework and<br>effective control mechanisms to counter cyber security risks<br>and data protection principles. Smart contracts should be<br>extensively tested before deployment using various scenarios<br>and stress tests. The ledger and smart contracts should<br>implement strong access controls to ensure only permitted<br>persons can modify or interact with the ledger/smart<br>contracts. Human intervention should be integrated into the<br>workflows at critical points for added security.|



_Table 2: Design principles for tokenised bank liabilities_ 

01/07/2025 

18 

--- end of page.page_number=18 ---

## **4 Operating models and considerations** 

## **4.1 Changes in operating practices** 

As the operating models evolve, this in turn will necessitate changes to existing operational practices: 

## _**Adoption of Tokenised Bank Liabilities in Transaction Banking**_ 

Payment processes shift with the adoption of tokenised bank liabilities, moving away from traditional payment methods involving the debiting and crediting of bank accounts and may rely on on-chain wallet addresses for settlements. While shared ledger-based models offer potential efficiency gains and reduced reliance on intermediaries, certain constraints remain. 

Time zone mismatches remain a key hurdle, requiring coordination between financial institutions operating in different settlement timetables. For example, today’s fiat currency FX spot settlement generally occurs on a T+2 basis to account for time zones, the differing operating hours of domestic RTGS systems and the need to coordinate the activities of correspondent banking relationships. T0 settlement is possible for currencies where the time-zone and RTGS systems are operating. The use of tokenised bank liabilities does not, at least at these initial stages, resolve these time zone issues and the need to prefund. 

Co-existence of emerging shared ledger-based platforms with legacy systems is expected to persist for some time. This overlap means that banks and financial institutions must maintain hybrid models that integrate tokenised bank liabilities with traditional infrastructure to allow co-existence and scalability. These hybrid models will be instrumental in ensuring institutions can leverage the efficiencies of shared ledgers while retaining interoperability with legacy fiat-based systems during the adoption phase. 

Ultimately, as familiarity and sophistication improve owing to wider adoption over time, on-and-off-chain ramps (fiat-to-token exchanges) are currently relied upon as the operating models for the co-existence of fiat deposits and digital money (e.g., tokenised bank liabilities). These ramps will enable institutions to move funds seamlessly between shared ledgers and legacy systems. They also supplement critical workflows related to token exchanges into fiat, reconciliation between off-chain and on-chain balances, ensuring integration with RTGS systems to support compliance and alignment with existing payment standards.  As shared ledgers gradually matures in its adoption, the operating systems for the interaction between fiat deposit and digital money would likely evolve over time. 

## _**Clarity on the Recognition of Tokenised Bank Liabilities in Different Jurisdictions**_ 

Effecting cross-border settlements directly to wallets maintained in different jurisdictions will require a degree of legal alignment or mutual recognition regarding the legal nature of tokenised bank liabilities across these jurisdictions. At this point, existing case law on virtual assets remains limited, and the legal characterisation of tokenised bank liabilities does not appear to be settled. Additionally, there are concerns that the absence of formal legal recognition of tokenised bank liabilities in some jurisdictions may affect their function as a stable store of value and limit the availability of suitable token-issuing deposit banks, 

01/07/2025 

19 

--- end of page.page_number=19 ---

particularly for emerging market currencies. As developments in this area continue to evolve, the legal nature of tokenised bank liabilities may vary across jurisdictions, with different countries taking diverse approaches based on their financial regulatory frameworks, legal traditions (common law vs civil law), and progress in digital asset regulation. 

## _**Integration of Shared Ledger in Accounting Practices**_ 

Wider use of tokenised bank liabilities will require accounting standards and banking general ledgers (GLs) to accommodate the recognition, measurement and reporting of realtime shared ledger-based payments and balances. Additionally, there may be challenges in handling tokenised bank liabilities with additional functionalities attributed to the programmability of such tokens (e.g., embedded smart contract logic, automated checks or interest accrual features). 

Liquidity fragmentation also further complicates accounting, as institutions must reconcile fiat and token balances across time zones to ensure operational smoothness. Operational setups will therefore include automated reconciliation mechanisms to match digital and fiat balances, while minimising inefficiencies and opportunity costs associated with idle tokenised liquidity. 

## _**Evolving Role of Correspondent Banks**_ 

As tokenised payments become more widespread, the role of correspondent banks may also change as it may lead to reduced intermediation between institutions. 

However, correspondent banks are uniquely equipped to bridge shared ledger networks with legacy systems by facilitating fiat-to-token exchanges, providing deposit account services, and offering credit solutions. Additionally, correspondent banks could further enable shared ledger adoption by participating in multi-participant networks (such as shared ledger PvP Orchestration networks) or act as liquidity providers to facilitate the intermediation between token issuers, currencies and shared ledger networks. 

## _**Currency Conversion Practices**_ 

The transition from direct fiat currency conversion to exchanges between tokens denominated in various currencies may require adapting operating standards particularly for sourcing and application of currency reference. At the initial stages of adoption, financial institutions would need to depend on off-chain external benchmarks integrated through automated systems and on the shared ledgers via price oracles. 

In the long term, as the shared ledger ecosystem meets key prerequisites, it is possible that financial institutions could transition to native on-chain FX rate discovery, leveraging aggregated transaction data to determine pricing. 

## _**Standardisation of Smart Contracts**_ 

For payments made across shared ledgers or financial institutions, standardised specifications will be needed to ensure the interoperability of payment requests across different shared ledger networks. 

01/07/2025 

20 

--- end of page.page_number=20 ---

## _**On-Chain Settlement Standards**_ 

If in due course settlement becomes integrated, standardised processes will have to be established for settlement directly on the shared ledger. These standards will address critical requirements, such as settlement finality, interoperability, and legal certainty, to ensure reliability in payment flows. Settlement finality refers to the legally defined point in time when a payment or transfer becomes final, unconditional, and irrevocable. It is essential to prevent risks such as clawbacks due to insolvency, which could undermine the stability of the payment system. Alongside operational rules, the laws of relevant jurisdictions must be reviewed to confirm that legal principles governing settlement finality extend to shared ledger-based payments, such as those involving tokenised commercial bank deposits. Where necessary, legal opinions should be obtained to clearly define the point at which finality occurs. In many jurisdictions, specific legislation has already been enacted to ensure settlement finality in traditional systems, but further adaptation may be needed for shared ledger-based models. 

The development of shared ledger-based settlement frameworks, particularly for crossborder transactions, will require careful legal reviews to ensure settlement finality is upheld across diverse jurisdictions. In some cases, transaction banking involving tokenised bank liabilities may continue to operate within existing RTGS systems. For these implementations, legal reviews might conclude that settlement finality is already supported under current laws. However, this may not be the case for newer payment models which rely entirely on decentralised networks. 

Establishing proper scale requires standardisation across the industry. The Global Layer One (GL1) initiative[22] exemplifies this by developing an ecosystem of market infrastructures that aligns with regulatory requirements. It focuses on creating common standards for governance, risk management controls, and settlement arrangements for cross-border transactions. 

GL1’s primary goal is to establish financial market infrastructure standards and specifications that will govern how GL1-compliant platforms operate. This framework enables institutions to validate their services against internationally recognised principles and meet regulatory requirements across different jurisdictions. GL1 specifically details the necessary controls for financial market infrastructures running shared ledger infrastructure, while providing guidance on addressing compliance gaps to meet these standards. 

## _**Regulatory and Compliance Adaptations**_ 

Anti-Money Laundering (AML) checks and transaction monitoring will remain essential to enable banks to comply with applicable banking requirements albeit that these will have to be integrated to the on-chain environment (at the pre-trade, trade and post-trade stage). 

Compliance frameworks will evolve to leverage the shared ledger’s inherent transparency, and financial institutions to streamline AML/KYC obligations while maintaining regulatory rigor. While shared ledger can enhance on-chain monitoring and validation processes, tokenised bank liabilities must continue to comply with off-chain checks, such as verifying 

> 22    Global Layer 1 (MAS) (2024). https://www.mas.gov.sg/publications/monographs-or-information-paper/2024/gl1-whitepaper 

01/07/2025 

21 

--- end of page.page_number=21 ---

the source of funds, in accordance with established anti-money laundering laws and global regulatory standards. 

Operational practices will progressively utilise the shared ledger’s auditability and immutability to supplement traditional compliance methods. These hybrid frameworks will balance the efficiency gains enabled by shared ledger technology with the risk management requirements of regulatory oversight. By integrating regulatory workflows into tokenised systems, financial institutions can achieve a seamless merger of innovation and compliance, safeguarding trust and integrity within the financial ecosystem. 

GL1’s Programmable Compliance Toolkit[23] , demonstrates how jurisdiction-specific regulatory requirements, including AML checks and capital flow management measures, can be encoded as conditions within programmable wrappers that hold tokenised assets. This approach enables real-time verification of compliance requirements, exemplifying regulatory oversight for transactions utilising shared ledger infrastructure. 

## _**Global Settlement Date and Point of Settlement**_ 

The concept of a “global trading date” (i.e. a globally consistent definition of a singular synchronised trading date) is of significant value in mitigating settlement risk, increasing the opportunity for PvP, and is expected to positively impact how counterparty risk exposure is measured and mitigated. It may also result in other ancillary benefits such as creating a framework for the development of an intraday FX swap market. 

However, the shift towards 24/7 payment capability may require the concept of “Settlement Date” to be re-considered. There is potential to align the “Settlement Date” concept with the concept of a “global trading date”. A “global settlement date” will require careful consideration given the significantly wider implications to traditional financial instruments, such as FX, interest rates and other asset classes. Needless to say, there will also be profound impact on downstream products (e.g., derivatives) and their associated processes. 

Closely linked to this is the point of settlement, where the settlement of an FX transaction is often determined by the reconciliation of end-of-day agent bank statements to confirm receipt of funds, i.e., confirms settlement finality has occurred. Transitioning to a framework based on a “global settlement date” would transform how settlement finality is confirmed and processed. An extended, globally consistent timeline would necessitate automated reconciliation mechanisms such as the use of timestamps to ensure timely confirmation of settlement across jurisdictions. 

A timestamp could be an important technical development in the management of real-time payments, allowing real-time credit management and ultimately freeing up funds for other purposes, such as investment. 

> 23 Global Layer 1 (GL1) (2024), Programmable Compliance Toolkit. https://doc.global-layer-one.org/docs/programmablecompliance/overview/introduction. 

01/07/2025 

22 

--- end of page.page_number=22 ---

## _**Set-Up Costs, Operational Challenges, and Capital Considerations**_ 

At the initial stages, participants must account for setup costs related to system connectivity and account structures. Separate balances will also need to be maintained across fiat and digital accounts, leading to increased funding costs. In certain cases, the need to ensure settlement across multiple time zones may require pre-funding accounts ahead of time, resulting in excess balances in these accounts. These balances typically do not earn interest, leading to opportunity costs that could impact FX rates. While these challenges present initial hurdles, they are likely to be addressed as digital money and tokenised bank liabilities mature. 

Furthermore, deposit takers must carefully evaluate the capital and liquidity impacts of issuing tokens backed by bank liabilities. The conversion of bank liabilities into tokens can affect liquidity coverage ratios (LCR), as such bank liabilities may need to be allocated to low-risk liquid assets, such as high-quality liquid assets (HQLAs). This reduces the deposit takers’ effectiveness in supporting economic financing and maturity transformation compared to traditional bank liabilities. 

## _**Liquidity Providers Operational and Governance Considerations**_ 

Liquidity providers may face several challenges in supporting tokenised payments for settlements, including ensuring sufficient liquidity in applicable tokens prior to transactions and developing processes for minting and burning tokens to manage liquidity pools. Legal arrangements between providers and banks remain untested, necessitating the creation of standardised contracts. 

In multi-provider scenarios, criteria for provider selection, governance standards, and selection mechanisms will need to be defined to ensure effective management and transparency. 

## **4.2 Potential operating models** 

Tokenised bank liabilities in different currencies and issued by different deposit takers need a way to be exchanged to ensure the transfer of funds from one country to another can be completed end-to-end. The case studies considered in more detail in Section 6 illustrate the various solutions that have been devised to achieve this, though these are mainly at an experimental stage. In summary these are: 

- the use of a liquidity provider to swap between tokens in different currencies issued by different issuers (Use Case 1); 

- the use of “Hash-Time Locked Contracts” to create interoperability between two private permissioned shared ledger networks (Use Case 2). In this model, tokens are not exchanged but funds are held in escrow and are released at a certain defined point using smart contracts; and 

- the use of shared ledger-based PvP Orchestration – OSTTRA, an industry leading shared ledger platform which operates to facilitate the PvP settlement by participants of obligations arising under their bilateral FX transactions (Use Case 3). 

01/07/2025 

23 

--- end of page.page_number=23 ---

## **5 Risk considerations and mitigants** 

As adoption of shared ledger-based payments and settlement gain traction, they introduce a new range of risks across the lifecycle—from adoption to operational maturity. While the design principles outlined in Section 3 provide a robust baseline for managing these risks, practical considerations must address real-world implementation and system evolution. 

## **5.1 Risk Considerations** 

|**Area**|**Sub-Area**|**Risk Considerations**|**Potential Solutions**|
|---|---|---|---|
|**Strategic &**<br>**Market Risks**|Operational<br>Transition<br>Issues|Running parallel digital and<br>fiat systems introduces<br>complexity, with potential<br>reconciliation mismatches.<br>Fragmented liquidity may<br>impair FX execution.|Invest in seamless<br>integration between<br>legacy systems and<br>tokenised solutions to<br>reduce friction and<br>collaborate with liquidity<br>providers to mitigate<br>fragmentation.|
||Liquidity Risk|Low liquidity in non-major<br>currency pairs could affect<br>FX execution and<br>settlement.|Enhance liquidity pools<br>and partner with key<br>liquidity providers to<br>mitigate fragmentation in<br>FX markets, ensuring<br>smoother execution even<br>for smaller or less traded<br>currency pairs.|
|**Technological**<br>**Risks**|Smart Contract<br>Risks|Smart contracts may<br>malfunction, have bugs, or<br>behave unpredictably, and<br>immutability may prevent<br>reversal of erroneous<br>transactions.|Prioritise comprehensive<br>smart contract audits,<br>continuous testing across<br>various scenarios, and<br>monitoring to identify and<br>correct errors swiftly.|
||Cybersecurity<br>Risks|Risks of malicious actors<br>exploiting system<br>vulnerabilities due to<br>unpatched flaws or<br>cryptographic<br>breakthroughs.|Implement robust<br>cybersecurity measures,<br>regular vulnerability<br>assessments, and<br>adherence to best<br>practices in cryptography<br>for optimal protection.|
||Data Integrity<br>Risks|Ledger forks,<br>malfunctioning nodes, or<br>errors in the codebase may<br>compromise the integrity<br>of transaction records.|Adopt reliable, well-<br>maintained ledger<br>solutions and implement<br>strong data verification<br>processes to ensure|



01/07/2025 

24 

--- end of page.page_number=24 ---

|**Area**|**Sub-Area**|**Risk Considerations**|**Potential Solutions**|
|---|---|---|---|
||||accuracy and integrity in<br>all transactions.|
|**Operational**<br>**Risks**|Payment &<br>Settlement<br>Mechanisms|Complexities arise when<br>converting tokenised<br>assets to fiat, and systemic<br>risks increase with delayed<br>finality.|Integrate tokenised<br>payment rails with<br>existing financial systems<br>and conduct rigorous<br>testing for potential<br>failure points to ensure<br>smooth settlement<br>processes.|
||Interoperability<br>Risks|Risk of fragmentation or<br>delays if platforms and<br>protocols cannot reliably<br>interact.|Prioritise building<br>interoperable platforms<br>that can reliably<br>communicate across<br>different protocols and<br>payment networks,<br>reducing potential delays<br>in cross-platform<br>transactions.|
||Third-Party<br>Providers Risks|Dependencies on third-<br>party service providers<br>(e.g., cloud services) may<br>introduce risks of failure or<br>latency.|Ensure thorough due<br>diligence and have<br>contingency plans in place<br>for third-party risks,<br>evaluating providers for<br>reliability and scalability.|
|**Legal &**<br>**Regulatory**<br>**Risks**|Jurisdictional<br>Variability|Tokenised instruments<br>may not be uniformly<br>recognised under law,<br>particularly in cross-border<br>FX use cases.|Engage with regulatory<br>bodies early to influence<br>the development of clear<br>legal frameworks,<br>ensuring compliance and<br>helping shape policy.|
||Evolving<br>Regulations|Shifting regulatory<br>landscapes could affect the<br>legal enforceability, tax<br>treatment, and<br>documentation standards<br>for tokenised solutions.|Maintain flexibility in<br>internal processes to<br>adapt to evolving<br>regulations, ensuring<br>ongoing compliance and<br>minimising operational<br>disruptions.|
|**Disclosures**|Platform<br>Transparency|Limited visibility into<br>shared ledger platform<br>governance, technical<br>operations, or|Enhance transparency<br>regarding platform<br>governance and key<br>intermediary roles to<br>improve trust and|



01/07/2025 

25 

--- end of page.page_number=25 ---

|**Area**|**Sub-Area**|**Risk Considerations**|**Potential Solutions**|
|---|---|---|---|
|||intermediary roles may<br>impede risk assessment.|facilitate accurate risk<br>assessments.|
||Sustainability|Certain shared ledgers<br>consensus mechanisms<br>have a significant carbon<br>footprint, potentially<br>conflicting with<br>sustainability goals.|Consider adopting more<br>energy-efficient<br>consensus mechanisms<br>and promoting<br>sustainability in shared<br>ledger solutions.|



_Table 3: Risk considerations in implementing shared ledger-based payments and settlement_ 

## **5.2 Risk Mitigants** 

Drawing from the Guardian Fixed Income Framework paper published in 2024[24] , this paper also elaborates on and adapts key risk mitigants for broader application in tokenised markets. 

|**#**|**Risk Mitigant**|**Description**|
|---|---|---|
|1|Ensure Robust<br>Internal Compliance<br>Functions|Develop and scale internal risk, compliance, and control<br>frameworks to align with the growing complexity of<br>tokenised systems, ensuring continuous monitoring and<br>adaptability.|
|2|Ensure an Effective<br>Incident Response<br>Mechanism|Establish clear, proportionate protocols to address faults in<br>smart contracts or shared ledger discrepancies, minimising<br>potential disruptions.|
|3|Ensure Standardised<br>Smart Contract<br>Audits|Implement recognised audit frameworks and conduct<br>independent security reviews before deployment to<br>proactively identify and rectify vulnerabilities.|
|4|Ensure Clear &<br>Documented Code|Write smart contracts in well-commented, transparent<br>code that facilitates routine audits, maintenance, and<br>updates.|
|5|Ensure Extensive<br>Pre-deployment<br>Testing|Conduct comprehensive simulations and stress tests under<br>diverse scenarios to validate smart contract behaviour and<br>operational resilience before live deployment.|
|6|Ensure a Combined<br>Access & Manual<br>Review Framework|Utilise a unified approach that integrates automated access<br>controls with human intervention to monitor and verify key<br>system functions, reducing the risk of error or malicious<br>activity.|
|7|Ensure Contractual<br>Clarity Among<br>Stakeholders|Clearly define legal roles, responsibilities, and liabilities<br>among all parties through robust contractual agreements<br>to guarantee enforceability and mitigate disputes.|



> 24     https://www.mas.gov.sg/publications/monographs-or-information-paper/2024/guardian-fixed-income-framework 

01/07/2025 

26 

--- end of page.page_number=26 ---

||8|Ensure a Robust<br>Legal Framework for<br>Derivatives|When a technology solution is applied to derivatives<br>trading, there are risks that the resulting contract may lack<br>legal efficacy. To address this, ISDA has published a series of<br>guidelines for smart derivatives contracts—including one<br>on FX derivatives—that explain the core principles of the<br>ISDA documentation and raise awareness of the key legal<br>terms that must be maintained. These guidelines also<br>highlight important issues for technology developers when<br>designing solutions for trading, processing FX, or<br>automating settlement, and point to areas where further<br>industry collaboration is needed to resolve legal and<br>regulatory uncertainty.|
|---|---|---|---|



_Table 4: Key risk mitigants for broader application in tokenised markets_ 

01/07/2025 

27 

--- end of page.page_number=27 ---

## **6 Case studies and examples** 

In this section, we explore case studies of implemented solutions that demonstrate the use of tokenised bank liabilities, more specifically tokenised deposits in transaction banking. 

A tokenised deposit[25] is a digital representation of a commercial bank deposit issued and managed on a shared ledger. The main reason for leveraging money on deposit is its affinity with the traditional two-tier monetary system and with existing laws and regulations, meaning that innovation can take place within the existing framework of the financial system in a more straightforward way. 

The terminology of tokenised deposit is used in this paper in a broad, functional sense to refer to tokenised representation of commercial bank deposit on a shared ledger, without asserting any specific legal or regulatory interpretation. 

## **6.1 Use case 1:  Ant International - multi-currency tokenised deposit for crosscurrency FX payments[26]** 

## _**Overview**_ 

Ant International, a leading global provider of digital payment and financial technology solutions, supports merchants worldwide of all sizes in achieving their growth goals. Through a comprehensive range of technology-driven services, Ant International collaborates closely with partners to initiate and receive payments across multiple locations 24 x 7. Partnering with over 70 global financial institutions, Ant International provides online payment channels serving 1.2 billion buyers and 2 million sellers in more than 200 countries, supporting major global merchants and all Alibaba affiliates. 

By building a distributed financial network, Ant International seeks to transfer fiat currency and on-chain tokenised assets globally with virtually no delay, significantly enhancing liquidity and operational efficiency. This should reduce the costs associated with traditional cross-border payments—transforming a process that could take between one to three days and costing up to $27 on average to complete (excluding FX cost)[27] into one that is instant and cost-effective—but also dramatically shortens the time for funds to clear to minutes or even seconds. 

Cross-border payments where an exchange of currencies is required constitute a significant percentage in daily corporate treasury management, creating a demand for a technological solution through tokenised payments. 

Currently, Ant International partners with several Project Guardian participating banks on tokenised deposits issuance with the goal to perform 24 x 7 cross-border transactions for internal liquidity management.  However, this is faced by practical challenges including 

> 25   Tokenised deposits are liabilities of the bank that meet the definition and legal characteristics of a deposit according to the relevant legal framework of the jurisdiction in which the deposit is accepted. 

> 26  The focus of the pilot is on testing the technology and the token exchange model. It does not assert any specific legal or regulatory interpretation on whether the token is a tokenised deposit. 

> 27  https://www.oliverwyman.com/content/dam/oliver-wyman/v2/publications/2021/nov/unlocking-120-billion-value-in-crossborder-payments.pdf 

01/07/2025 

28 

--- end of page.page_number=28 ---

- FX markets being available only 5 and a half days a week 

- Lack of standard definitions for participants’ role in tokenised deposits 

## _**Business process and solution; the role of a liquidity provider**_ 

A cross-border payment may involve the exchange of tokenised deposits in different currencies, potentially issued by different issuers. To address this cross-currency element, Ant International is piloting an approach for a token exchange model with a “Liquidity Provider” (further described below) to facilitate cross-border payment, while leveraging banking partners to provide off-chain FX pricing through a price oracle. For the pilot use case, Ant International acts as the liquidity provider, but this role can be potentially filled by another market participant, with the relevant licensing requirements, as the solution matures. 

A liquidity provider’s role is to perform a token exchange between tokens in two currencies and provide a fixed quote price to the end user. In this use case, a banking partner will be providing an off-chain FX price to the price oracle. With this, tokens denominated in different currencies and by different issuers will then be used to complete the cross-border payment. 

To become a liquidity provider: 

- The liquidity provider must be a legal holder of both tokens in the currency pair and possess a certain amount of each token. 

- The liquidity provider must have the capability to offer token currency pair pricing, securely, reliably, and in real-time, by using a price oracle to upload the off-chain token pair exchange rates onto the shared ledger. 

An FX quote oracle provides secure transmission of on-chain and off-chain prices, while the on-chain Multi-Token Swap (MTS) contract facilitates the exchange of different assets and supports automatic AML screening through a user whitelist maintained by the MTS service provider to meet regulatory compliance requirements. 

01/07/2025 

29 

--- end of page.page_number=29 ---

**==> picture [398 x 301] intentionally omitted <==**

_Figure 2: Illustration of on-chain MTS smart contract interaction flow_ 

The payment effectively takes place in 3 main stages – issuing, transferring and redeeming; with the token exchange occurring at the transfer token stage. 

**==> picture [404 x 169] intentionally omitted <==**

_Figure 3: Illustration of payment flow._ 

|**Stage**|**Activity**|
|---|---|
|Issue Token|1. Issuer Bank A in Singapore off-chain debits Ant<br>International’s Entity A cash account for SGD fiat<br>currency<br>2. Issuer mints Bank A SGD token to Ant International’s<br>Entity A’s token address|



01/07/2025 

30 

--- end of page.page_number=30 ---

|Transfer Token|3. SGD token is transferred to the liquidity provider’s<br>token address<br>4. The liquidity provider provides price quote, checks for<br>currency pair availability, relevant screening and<br>performs token exchange (SGD token for USD token).<br>The liquidity provider then transfers USD token to Ant<br>International Entity B’s token address|
|---|---|
|Redeem Token|5. Issuer Bank B burns USD token from Ant International<br>Entity B’s token address<br>6. Issuer Bank B credits Ant International Entity B’s cash<br>account with USD fiat currency.|



_Table 5: Description of payment flow._ 

With this setup, Ant International is able to perform a cross-border cross-currency payment within internal entities. 

## _**Key learning points and potential future development**_ 

A token exchange model using a liquidity provider is a potential solution for cross-border payments using tokenised deposits. Leveraging smart contracts, the liquidity provider can perform on-chain fulfilment of the token swap, ensuring transparent, immutable and secure transactions occur in real-time. In addition, programmability embedded in the tokens, such as conditional payments, would be able to enhance transaction efficiency and flexibility. For example, conditional payments can automate processes such as releasing funds only when predefined conditions are met, reducing the need for intermediaries, lowering cost, and mitigating risks of disputes. This programmability can also enable features like automated compliance checks, escrow arrangements, or milestone-based disbursements, all of which can streamline operations. While liquidity providers are rewarded with liquidity cost and price spread, the entry of more liquidity providers will unlock additional liquidity to the market. Additionally, liquidity providers could exchange tokenised deposits with each other, creating a more robust and interconnected liquidity network. This would further enhance market efficiency by enabling seamless transfers and price discovery across different currencies and platforms. 

For future development, the scope can be expanded to study the feasibility of interoperability with existing FX trading systems. Additionally, as the size of the tokenised deposit market grows in the future, the implication of expanding the liquidity provider model to more participants can be studied, together with its licensing and technology setup requirements. 

## **6.2 Use case 2: BNY and OCBC - FX payments through shared ledger interoperability** 

## _**Overview**_ 

BNY and OCBC use case focused on shared ledger interoperability, as the proliferation of new networks with distinct value propositions has led to a fragmented landscape for new clearing and settlement locations, with no clear winner yet. 

01/07/2025 

31 

--- end of page.page_number=31 ---

To overcome this and still drive shared ledger adoption, BNY Treasury Services and OCBC collaborated to demonstrate how “Hash-Time Locked Contracts” (further described below) could create interoperability between two private permissioned, bank owned shared ledgers. Each bank operates their own shared ledger as they would today but leverages the technical benefits of modern infrastructure to increase the speed of transaction and the security with which the transaction is processed. 

The pilot proved the technical feasibility of this solution, including near-instantaneous settlement of the transactions (vs. typical cross-border FX payment transactions, which can take up to 2 days to settle). The model provides the immediate benefit of faster settlement speeds for clients, while providing a roadmap for financial institutions to adopt the new technology. In addition to the speed and efficiency improvements demonstrated by the pilot, BNY and OCBC plan to explore design concepts for shared ledger-based fraud mitigation tools that can increase both banks’ ability to identify and flag suspicious activity in near real time. 

## _**Business process and solution description**_ 

The originating bank initiates a transaction as they would today, then passes a message via a bilateral connection to the beneficiary’s bank (or correspondent). The payment message will include a secret, which in turn unlocks the smart contract holding the funds in escrow. Once the funds are unlocked from escrow, they are credited to the beneficiary’s account in tokenised deposit form, and then made available for “last mile” pay-out through traditional instant payment rails. 

The solution relies on private, permissioned shared ledger at both participating financial institutions, as well as a smart contract standard using Hash Time-Locked Contracts to exchange either messages or tokens across the two chains. There is no need for each bank to run on the same type of underlying shared ledger technology (e.g., Hyperledger Besu vs. R3 Corda) in this case, as the tokens are not swapped; only messages exchanged (in the pilot phase). 

**==> picture [404 x 76] intentionally omitted <==**

_Figure 4: Illustration of the business process and solution description_ 

## _**Key Learnings and future developments/possibilities**_ 

The bilateral connection between the two banks can be replicated across strategic partners, globally. This solution offers an alternative to both existing messaging networks, and the need for a central, single coordination point, enhancing the resiliency of both banks’ operations. There are critical opportunities to introduce new fraud mitigation tools into the process given the “speed bump” introduced by the escrow of funds in the HTLC. Making 

01/07/2025 

32 

--- end of page.page_number=32 ---

payments faster and safer is critical to BNY and OCBC’s strategy and the industry’s continued growth. 

## **6.3 Use case 3: HSBC - Payment vs Payment orchestration** 

## _**Overview**_ 

Cross-border FX settlement for inter-bank FX trades can be complex and manual due to different risk management and payment systems. There are industry needs for safer and instantaneous settlement with reduction of settlement and credit risks, improved liquidity pools, and capital efficiencies. A multi-participant PvP solution using shared ledger will be able to mitigate the challenges. This solution also supports alignment with FX Global Code Settlement Risk Principles 35 and 50, the Financial Stability Board’s G20 roadmap for enhancing cross-border payments, and the CPMI’s Stage 2 report to the G20. 

OSTTRA – an industry leading post trade infrastructure solution, provides service offering for participants to match, confirm and pay FX cash flows (PvP) with reduction of Herstatt risk and flexible settlement windows. Outstanding exposures can continuously be netted to reduce daily settlement limits and unlock capital, allowing more trade volumes to be conducted. 

HSBC’s key role will be as a full supporter and a network participant of this solution across all supported currencies including emerging market currencies (e.g., CNH). HSBC has been using this solution internally for over 6 years, settled over 9.4 Tn USD, and have realised the benefits of using PvP amongst 18 entities. 

**==> picture [406 x 148] intentionally omitted <==**

_Figure 5: High level illustration of solution design_ 

There are several reasons why the solution has been successful and operational for over 6 years: firstly, the solution relies on fiat money using existing bank account structures; secondly, the solution is an overlay to existing systems and solutions used by the industry; and thirdly, the implementation of shared ledger as a smart workflow and payment orchestration layer leverages existing risk and control frameworks. 

Current industry challenges also include interoperability across multiple platforms and networks, increasing ecosystems’ complexity and hence operational risks. Migration from legacy to digital can also be costly and takes time. Trading counterparties may not have full 

01/07/2025 

33 

--- end of page.page_number=33 ---

visibility of their forward-looking FX cash flows which may result in missed payments and overdraft fees. 

Benefits of the OSTTRA PvP solution (by joining as a network participant) will help to solve the following: 

- Lower costs by removing confirmations, reconciliations and external fees. 

- Single view of settlement lifecycle across multiple global systems. 

- Transparency of forward-looking FX cash flows. 

- Full audit trail from trade capture to cross-border settlement. 

- Reduced Herstatt risk through utilisation of shared ledger to orchestrate PvP settlement. 

- Lower implementation costs, as it’s an overlay of existing infrastructure. 

- Direct API integration available to allow transfer of funds within secs. 

Future benefits will be enablement of cash flow compressions of outstanding exposures using ‘settle to market’ payments to reduce counterparty risks and capital requirements. 

## _**Business process and solution - straight through processing solution**_ 

The PvP process is designed to follow straight through processing (STP) after onboarding and connectivity has been set up with the OSTTRA shared ledger network. Operational staff will have full graphical user interface (GUI) dashboard for monitoring of the trade lifecycles from trade matching to funding, PvP, and de-funding before completion of the day end obligations. Exceptional management processes are available if counterparties wish to settle outside of the network bi-laterally. The detailed steps are as follows: 

- Trading counterparties send their trades to OSTTRA in real time. 

- Trades will be matched by OSTTRA before inclusion in the netting set by currency and counterparty. 

- On settlement day, pre-settlement netting amounts are matched and agreed by participants which triggers messages to participants’ post trade systems for funding of their obligations. 

- Once funds are received from both sides, a PvP event will occur to reflect change of fund ownership (funds are transferred between trading counterparties’ accounts only, OSTTRA is not the intermediary, only acting as an orchestrator of the process). 

01/07/2025 

34 

--- end of page.page_number=34 ---

- Funds will move back to designated nostros before market cut-off times. 

**==> picture [367 x 235] intentionally omitted <==**

_Figure 6: Illustration of business and solution flow_ 

- Participants trading system will need to send trades to OSTTRA in real time. 

- Participants’ post-trade systems will need to be connected (e.g., MQ) to OSTTRA to receive instructions for trade confirmation and funding events. 

OSTTRA service relies on existing network for trade matching and shared ledger (provided by Baton Systems) for record of obligations and orchestration. 

## _**Key Learnings and future developments/possibilities**_ 

This proposed solution needs a network of participants to realise the benefits. With more emphasis on conducting safer and instantaneous cross-border payments, a shared ledger PvP solution will have a significant role to play for reduction of bi-lateral settlement and counterparty risks. It can also provide better liquidity and a reduction in capital requirements, unlocking more trading and efficiencies. 

Direct API connectivity with participants also helps to reduce the current network messaging (SWIFT) dependencies & delays from minutes to seconds. 

OSTTRA (network operator) is already connected with many wholesale banks and corporates from their existing solutions. With their open APIs, onboarding to this network as a participant will cost less to implement, it’s an overlay to existing infrastructures. 

As the market is trying to solve interoperability, this proposed pilot and recommendation to join can help to solve many of the current challenges. 

While the suggested use case is mainly for FX related cash flows, this solution is asset class agnostic and so it can easily be expanded for cross asset cash flows in the future. 

01/07/2025 

35 

--- end of page.page_number=35 ---

## **7 Developing standardised documentation for tokenised FX transactions** 

The use cases above provide illustrations on possible implementation and potential benefits through the adoption of tokenised bank liabilities and shared ledger solutions. An industry-wide framework that is broadly accepted will accelerate technology adoption by reducing legal uncertainty and standardising operations. This standardisation will help achieve consistency, transparency and efficiency across markets. 

## **7.1** 

## **Existing industry standards** 

Associated standards and frameworks have been developed in the FX market to ensure transparency, efficiency and risk management. These standards are also applicable to transaction banking involving FX payments. They play a crucial role in fostering user confidence and the smooth functioning of the transaction banking market and will act as the foundation for further development alongside the increased use of tokenised bank liabilities. 

|**Standards**|**Examples**|
|---|---|
|Messaging and<br>communication|•<br>SWIFT Messaging (notably for post-trade and asset<br>servicing).<br>•<br>FIX Protocol (notably for trading)<br>•<br>Adoption of ISO 20022 XML format for trade reporting|
|Standard identifiers<br>for trades and<br>parties|•<br>Examples of instrument identifiers include Unique Trade<br>Identifiers (UTI) and Unique Product Identifiers (UPI)<br>•<br>Legal Entity Identifier (LEI).|
|Settlements|•<br>Settlement cycles – T+2 for the vast majority of currencies<br>and T+1 for USD/CAD, USD/TRY, USD/PHP and USD/RUB.<br>•<br>Ideally, PVP settlement and ideally net settlement to<br>alleviate daylight (or Herstatt) risk.28|
|Regulatory<br>compliance and<br>reporting obligations|•<br>Regulatory compliance and reporting are essential for<br>market integrity, customer protection, and financial<br>stability. Examples include:<br>-<br>Trade reporting for example the MFID II pre and post<br>trade reporting in the EU and Dodd-Frank Act in the<br>US OTC derivatives products, for which ISDA has<br>developed the Digital Regulatory Reporting solution<br>using the common domain model|



> 28 The most effective netting solution is multilateral as provided by CLSSettlement which settles over USD 6.5TN of payments daily on a PvP basis in 18 of the most actively traded currencies globally. This approach mitigates settlement risk and reduces funding costs by 96%. The alternative to multilateral netting is bilateral. In its December 2019 Quarterly Review, the BIS identified that of the USD 18.7TN daily gross payment obligations arising from all FX instruments, USD 6.3TN were settle on a PvP basis and USD 3.5TN were settled using bilateral netting. https://www.bis.org/publ/qtrpdf/r_qt1912.pdf 

01/07/2025 

36 

--- end of page.page_number=36 ---

|**Standards**|**Examples**|
|---|---|
||-<br>Business conduct disclosure standards, to which end<br>ISDA has published the Foreign Exchange Disclosure<br>Annex to the DFA Disclosure and other standards.|
|Industry best<br>practices|•<br>FX Global Code29|
|Documentation|•<br>ISDA Master Agreement<br>•<br>The 1998 FX and Currency Option Definitions jointly<br>published by ISDA, EMTA and the Foreign Exchange<br>Committee.|
|Messaging and<br>communication|•<br>SWIFT Messaging (notably for post-trade and asset<br>servicing).<br>•<br>FIX Protocol (notably for trading)<br>•<br>Adoption of ISO 20022 XML format for trade reporting|



_Table 6: Current Standards and Frameworks Governing the FX Market_ 

## **7.2 Ongoing industry initiatives and regulatory developments** 

Internationally, policymakers are leading efforts in exploring the use of tokenised bank liabilities and shared ledgers in settlements.  Apart from the BIS and FSB examples cited, other examples include: 

- **Germany** : The German Banking Industry Committee (GBIC) has published a whitepaper on the Commercial Bank Money Token (CBMT). 

- **Hong Kong** : The Hong Kong Government has launched Project Ensemble, a wholesale central bank digital currency (wCBDC) project to support the development of the tokenisation market in Hong Kong. One of the sandbox pilot strands is to encourage the use of tokenised deposits. 

- **Singapore** : The Monetary Authority of Singapore (MAS) has announced the development of an SGD Testnet to facilitate financial institutions access to common settlement assets for market testing purpose. 

- **South Korea** : In South Korea, a live pilot of tokenised deposits, involving 100,000 individuals, started during the October-December quarter of 2024. 

- **United Kingdom** : UK Finance, an industry group representing the financial services industry, has worked with a number of its members and partners on a new Regulated Liability Network (RLN) experimentation phase.[30] 

> 29 The FX Global Code is a set of global principles of good practice in the foreign exchange market, developed to provide a common set of guidelines to promote the integrity and effective functioning of the wholesale foreign exchange market. https://www.globalfxc.org/fx-global-code/. 

> 30 Further information available at: https://www.ukfinance.org.uk/news-and-insight/press-release/uk-finance-announces-successfuloutcome-regulated-liability-network. 

01/07/2025 

37 

--- end of page.page_number=37 ---

- **Project Agorá** , led by the BIS Innovation Hub, together with seven central banks and commercial banks from each jurisdiction, will test for improvements in the speed and cost of cross-border payments by utilising technologies such as tokenisation and “smart contracts”. 

## _**Development of consistent industry standards for data**_ 

To this end, consistent digital representation standards will also facilitate the development of smart contracts. The Common Domain Model[31] (“CDM”) is a standardised, machinereadable, and machine-executable data and process model for how financial products are traded and managed across the transaction lifecycle. Adoption of the CDM will enable a consistent hierarchical representation of trade data across trades, portfolios and events. It also allows for standard processing of trade lifecycle events, such as reporting under ISDA’s Digital Regulatory Reporting initiative, which significantly reduces the time, resources and cost needed to implement reporting regulations in multiple jurisdictions.[32] While the CDM was initially launched for derivatives, it is now used for repos, securities lending and bonds and is hosted by the Fintech Open Source Foundation (FINOS). 

## _**Value of standardised documentation**_ 

Standardised documents play a critical role in financial transactions by promoting consistency, transparency, and efficiency across markets. They reduce legal uncertainty by providing commonly accepted terms and conditions, which help parties understand their rights and obligations clearly. This consistency minimises the risk of disputes and litigation, fostering smoother negotiations and faster execution of transactions. Ultimately, it enhances market stability, reduces transaction costs, and promotes broader participation in global financial markets, especially in complex transactions like derivatives. 

## _**Developing standardised documentation for tokenised deposits**_ 

The Project Guardian FX workstream has identified that one of the practical challenges is the lack of standard terms in FX transactions using tokenised bank liabilities. Specifically for tokenised deposits, ISDA has been asked to develop industry standard documentation to facilitate use of tokenised deposits in FX transactions. One approach would be to leverage the existing ISDA documentation framework and develop model provisions (the “ **Additional Provisions** ”) for parties that wish to settle deliverable FX spot, forward, and swap transactions under an ISDA Master Agreement using deposit tokens, each denominated in a single fiat currency and issued or held on a shared ledger-based settlement platform. The Additional Provisions contemplate that such transactions will incorporate the definitions and provisions contained in the 1998 FX and Currency Option Definitions as published by ISDA, Emerging Markets Traders Association and the Foreign Exchange Committee (the “ **1998 FX Definitions** ”)[33] , or, as applicable, the 2021 ISDA Interest Rate Derivatives 

> 31  Further information about the common domain model is available at https://www.finos.org/common-domain-model. 

> 32  Further information about the ISDA Digital Regulatory Reporting initiative is available at https://www.isda.org/isda-solutionsinfohub/isda-digital-regulatory-reporting/. 

> 33  ISDA is in the process of updating the 1998 FX Definitions, with the industry implementation phrase set to run from late 2025 to November 2027. 

01/07/2025 

38 

--- end of page.page_number=38 ---

Definitions (the “ **2021 Definitions** ”) as published by ISDA. The chart below illustrates the ISDA documentation framework for FX transactions: 

**==> picture [403 x 153] intentionally omitted <==**

_Figure 7: FX and Currency Options Documentation Architecture_ 

## _**Changes to the FX Definitions to accommodate tokenised models**_ 

The Additional Provisions assume that the tokens in question record a sum of fiat cash in an account with the relevant participating banks, and are in registered or claims form. The liquidity provider will hold a pool of tokens from the participating banks which it will use to provide liquidity services to users. A user who holds tokenised deposits in one currency and wants to exchange it for another currency may enter into an FX transaction with the liquidity provider. The amendments assume that the liquidity provider and each user have signed, a 2002 ISDA Master Agreement and envisage that the transactions will be documented using a deliverable FX confirmation incorporating the 1998 FX and Currency Option Definitions 1998 FX Definitions as supplemented by these additional provisions. Although the Additional Provisions are drafted for the specific use case envisaged by the Project Guardian industry pilots, they may further be used as a reference and adapted as necessary, taking into account differences in token design and transaction structures. 

Adapting the “Additional Provisions” to accommodate tokenised models would require amendments to capture changes to timing, operating models, and the role of platforms. Preliminary studies indicate that these changes could potentially include: 

- changes to the “Business Day” definition to permit 24/7 settlement; 

- including a definition of “tokenised deposits” and clarifying that cash and currency includes tokenised deposits; 

- clarifying how payments under the transactions will be made in the context of tokenised deposits; 

- including, to the extent needed, contingency provisions to cater for platform related events; and 

- including representations from counterparties of their continued access to the platform in order to allow settlement on platform. 

01/07/2025 

39 

--- end of page.page_number=39 ---

## _**Enforceability of close-out netting and collateral arrangements**_ 

The enforceability of the ISDA Master Agreement and Credit Support Documents are supported by the netting and collateral opinions obtained by ISDA.[34] The principal focus of the opinions has always been on ensuring enforceability of netting and a related collateral arrangement against a party that is subject to insolvency proceedings. This is because mandatory insolvency rules come into operation that could potentially disrupt close-out netting and/or a related collateral arrangement. Applying existing insolvency law rules to a new asset class inevitably raises legal characterisation and other questions that must be tackled to provide the necessary certainty. ISDA has published a white paper[35] exploring the application of close-out netting to digital asset derivatives and the enforceability of collateral arrangements that involve transfers or exchanges of digital assets. The insolvency laws in each applicable jurisdiction should be considered in the context of tokenised assets. In the jurisdictions which have enacted specific legislation providing the legal basis for the issuance and ownership status of shared ledger-based tokens, the treatment of such tokens in an insolvency situation may be expressly catered for. In jurisdictions without specific enabling legislation, general insolvency laws principles will need to be applied. 

In the context of tokenised deposits, the nature of the customer’s rights against an issuer will lend to the nature of its claim against that entity in any insolvency proceedings. 

> 34 A list of the jurisdictions from which ISDA has obtained netting and collateral opinions appears on the ISDA website at www.isda.org, together with a list of the jurisdictions around the world that have enacted or are considering enacting netting legislation. 35 https://www.isda.org/2023/01/26/navigating-bankruptcy-in-digital-asset-markets-netting-and-collateral-enforceability/ 

01/07/2025 

40 

--- end of page.page_number=40 ---

## **8 Conclusion** 

The future of finance points towards an interconnected ecosystem where tokenised assets can be traded and settled globally, with settlement finality between counterparties. Achieving this vision requires progress on two fronts. First, the development of multipurpose shared ledger infrastructures that can support the exchange of tokenised assets and money while meeting regulatory expectations. Secondly, the development of robust connectivity protocols with liquidity providers serving as intermediaries to bridge different ledger platforms **[36] ,** and across multiple trading venues.[37] 

## _**Moving forward**_ 

Financial institutions would need to adapt their existing processes and infrastructures to prepare for the growing tokenised asset market. This paper has examined several use cases that demonstrate how FX transactions can integrate with tokenised asset infrastructures, highlighting how shared ledger networks can enhance operational efficiency. As both the technology and protocols mature, and as industry participants advance their applications, current interoperability challenges with existing systems will likely be resolved. 

Innovation and transformation in financial markets must extend beyond individual asset classes and institutions. Project Guardian provides a platform for industry participants to develop common standards across different capital markets products, enabling tokenised assets to scale sustainably and pool liquidity. 

The improved efficiencies achieved in transaction banking could generate broader benefits across financial market and enhance settlement processing for other asset classes[38] : 

- **Liquidity Optimisation:** Enhanced transaction banking efficiency could free up liquidity by reducing funds in transit and capital tied up in for pre-funding of payments. However, it is imperative to address the potential fragmentation of the liquidity pool and fungibility of liquidity pools arising from programmability of tokenised bank liabilities. 

- **Risk Reduction:** Streamlined cross-border payments and FX settlements could minimise counterparty and settlement risk, given that a common risk-free settlement asset and settlement finality could be jointly adopted by the market. 

- **Market Standardisation:** The adoption of tokenised bank liabilities by more market participants may encourage broader standardisation across the different systems, 

> 36 As discussed in the IMF Article _A Digital Marketplace to Improve Cross-Border Payments_ ; at https://www.imf.org/en/Publications/fintech-notes/Issues/2023/03/03/Trust-Bridges-and-Money-Flows-A-Digital-Marketplaceto-Improve-Cross-Border-Payments-528038. 

> 37 Another example is the Regulated Liability Network (RLN) proposal for a regulated financial market infrastructure that can deliver an interoperable network for various facets of the sovereign currency system: central bank money, commercial bank money, e- money and regulated stablecoins. https://regulatedliabilitynetwork.org/. The IMF (see paper in footnote 2) has considered a global clearinghouse to intermediate swap arrangements between central banks. 

> 38   The BIS Reports on Tokenised Assets has explored the implications of tokenisation on financial markets, including its potential to reduce settlement risks and enhance market efficiency. Studies on Continuous Linked Settlement (CLS) and Payment-versusPayment (PVP) systems have demonstrated how reducing settlement risk in one area of FX can positively influence broader markets. Various papers from SWIFT, ISDA, and financial institutions often discuss the cascading effects of technological adoption in financial services. 

01/07/2025 

41 

--- end of page.page_number=41 ---

creating a foundation for interoperability and consistency in processing settlements. This could have positive implications for how settlements are handled across multiple asset classes, including equities, commodities, and bonds. 

- **Broader Adoption of Digital Solutions:** Successful implementation in transaction banking use cases could act as a catalyst for wider industry adoption of tokenisation and shared ledger-based solutions, which could then drive improved efficiencies across different use cases benefiting from atomic payments and smart contracts. 

- **Increased Market Confidence:** Innovations that prove successful in digital payments including tokenised transfers may inspire greater trust and confidence in the scalability and reliability of such technologies. This, in turn, could attract participants in other asset classes to adopt these solutions. 

In conclusion, tokenised bank liabilities and payments have the potential to enhance efficiency, reduce risk, and improve liquidity in FX markets. Realising these benefits will require continued collaboration between industry participants and regulators to address key challenges, including the alignment of legal frameworks, the development of robust operational standards, and the refinement of regulatory treatment and compliance processes. 

While there are transitional challenges as traditional and emerging systems co-exist, these are not insurmountable. With thoughtful engagement and shared commitment, the industry is well-positioned to develop the clarity and best practices needed to support the safe and effective adoption of tokenised bank liabilities. As the ecosystem matures, these collective efforts will help lay the foundation for a more efficient, resilient, and inclusive global financial system. 

01/07/2025 

42 

--- end of page.page_number=42 ---

## **9 References** 

|1.|Financial Stability Board (FSB) (2024).G20 Roadmap for Enhancing Cross-border|
|---|---|
||Payments.|
|2.|Bank for International Settlements (BIS) (2022).Extending and aligning payment system|
||operating hours for cross-border payments.|
|5.|BIS (2023).Blueprint for the future monetary system: improving the old, enabling the|
||new.|
|6.|Ibid.|
|7.|FSB (2020).Enhancing Cross-Border Payments: Stage 3 Roadmap.|
|8.|CLSSettlement.CLS Settlement.|
|10.|Bank of England (BOE) (2024).Once more unto the breach.|
|11.|FSB (2024).FSB Annual Progress Report on Meeting the Targets for Cross-border|
||Payments, 2024 Report on Key Performance Indicators.|
|12.|BIS (2020). Payments without borders.|
|13.|BIS (2002).BIS Quarterly Review.|
|14.|Monetary Authority of Singapore (MAS) (2023). Project Guardian Open and|
||Interoperable Networks.|
|15.|Ibid.|
|17.|Monetary Authority of Singapore (MAS) (2023).Project Guardian Interlinking Networks.|
|19.|Monetary Authority of Singapore (MAS) (2024).Project Guardian Fixed Income|
||Framework.|
|22.|Global Layer 1 (GL1) (2024).Global Layer 1 Whitepaper|
|23.|GL1 (2024). Programmable Compliance Toolkit.|
|24.|Monetary Authority of Singapore (MAS) (2024).Project Guardian Fixed Income|
||Framework.|
|27.|Oliver Wyman, J.P. Morgan (2021).Unlocking $120 Billion Value In Cross-Border|
||Payments.|
|28.|BIS (2019).BIS Quarterly Review.|
|29.|Global Foreign Exchange Committee (2024).FX Global Code.|
|30.|UK Finance (2024).UK Finance announces successful outcome of Regulated Liability|
||Network Experimentation Phase.|
|31.|Fintech Open Source Foundation (FINOS).Common Domain Model.|
|32.|International Swaps and Derivatives Association (ISDA).ISDA Digital Regulatory|
||Reporting.|
|34.|ISDA (2025).Status of Netting Legislation.|



01/07/2025 

43 

--- end of page.page_number=43 ---

35. ISDA (2023). Navigating Bankruptcy in Digital Asset Markets Netting and Collateral Enforceability. 

36. International Monetary Fund (IMF) (2023). Trust Bridges and Money Flows: A Digital - 

Marketplace to Improve Cross Border Payments. 

37. Regulated Liability Network (RLN). (2022). The Regulated Liability Network Digital Sovereign Currency. 

38. BIS (2024). Report for the G20 on tokenisation highlights the opportunities, risks and future considerations for central banks. 

01/07/2025 

44 

--- end of page.page_number=44 ---

01/07/2025 

45 

--- end of page.page_number=45 ---



Final chunk number 0, content : 
01/07/2025 

1 

--- end of page.page_number=1 ---

## **Disclaimer** 

This report focuses on various design, operational and risk considerations in the use of tokenised bank liabilities and shared ledger in cross-border payments. The terminology of tokenised bank liabilities is used in this paper in a broad, functional sense to refer to tokenised representation of commercial bank money on a shared ledger, without asserting any specific legal or regulatory interpretation. This paper does not set out any positions on the legal classification or regulatory treatment of any tokenised bank liabilities which may vary by jurisdictions.


Final chunk number 1, content : 
This report and its contents are made available on an “as-is” basis without warranties of any kind. The content in this report does not constitute regulatory, financial, legal or any other professional advice and should not be acted on as such.


Final chunk number 2, content : 
None of its authors and contributors shall be liable for any damage or loss of any kind howsoever caused as a result from the use of the information contained or referenced in this report. This report does not seek to address policy objectives or recommend any specific solution or product. Whilst the content strives to provide more clarity on the subject matter, the authors of this report make no representation or guarantees on the performance or adequacy of the solutions or models.


Final chunk number 3, content : 
The examples used in the report are only for illustration purposes. The views and recommendations expressed are those by the lead contributors and do not imply a consensus view by market stakeholders across the spectrum of international debt capital markets. ## **Document Version** 

||Version|Date|Author|Rationale|
|---|---|---|---|---|
||1.0|July 2025|Guardian Foreign<br>Exchange Industry<br>Group|First publication|
||||||
||01/07/2025||||
|||2|||



--- end of page.page_number=2 ---

## **Acknowledgement** 

This is a joint report developed by the Guardian Foreign Exchange Industry Group. The report was led by Ant International, in collaboration with the International Swaps and Derivatives Association (ISDA). The report received additional contributions from organisations listed alphabetically: 

- Global Financial Markets Association’s Global Foreign Exchange Division (GFXD) 

- Monetary Authority of Singapore (MAS) 

- OCBC 

- The Bank of New York Mellon 

- The Hongkong and Shanghai Banking Corporation Limited (HSBC) 

01/07/2025 

3 

--- end of page.page_number=3 ---

## Table of Contents 

|**1**|**Introduction**...............................................................................................................................**5**|
|---|---|
|**2**|**Background**................................................................................................................................**7**|
|2.1|Cross-border payments and the FX market ........................................................................ 7|
|2.2|Differences between traditional transactions and transactions using tokenised bank|
|liabilities  ........................................................................................................................................... 7||
|**3**|**Implementation of shared ledger and tokenised bank liabilities in transaction banking**....**10**|
|3.1|Key lifecycle stages of traditional cross-border payments ...............................................


Final chunk number 4, content : 
10|
|3.2|Design Considerations ...................................................................................................... 15|
|**4**|**Operating models and considerations**....................................................................................**19**|
|4.1|Changes in operating practices .........................................................................................


Final chunk number 5, content : 
19|
|4.2|Potential operating models .............................................................................................. 23|
|**5**|**Risk considerations and mitigants**..........................................................................................**24**|
|5.1|Risk Considerations ........................................................................................................... 24|
|5.2|Risk Mitigants .................................................................................................................... 26|
|**6**|**Case studies and examples**.....................................................................................................**28**|
|6.1|Use-case 1: Ant International  .......................................................................................... 28|
|6.2|Use-case 2: BNY and OCBC ............................................................................................... 31|
|6.3|Use-case 3: HSBC ..............................................................................................................


Final chunk number 6, content : 
33|
|**7**|**Developing standardised documentation for tokenised FX transactions**.............................**36**|
|7.1|Existing industry standards ............................................................................................... 36|
|7.2|Ongoing industry initiatives and regulatory developments ............................................. 37|
|**8**|**Conclusion**...............................................................................................................................**41**|
|**9**|**References**...............................................................................................................................**43**|



01/07/2025 

4 

--- end of page.page_number=4 ---

## **1 Introduction** 

For the purposes of this paper, the terms ‘shared ledger’ and 'shared ledger infrastructure' refer to a common foundational infrastructure, such as a Distributed Ledger Technology (DLT) or blockchain network, where data is replicated, synchronised and shared across a network of participants. Transaction banking refers to the provision of instruments and services by a financial institution to corporate and institutional clients, enabling their day-to-day needs through payment processing (e.g., domestic and cross-border payments), cash management, trade finance, and securities services. In today’s global economy, participants in cross-border transactions are exposed to foreign exchange (“FX”) risks.


Final chunk number 7, content : 
FX is frequently used by corporates to settle payments in foreign currency or hedge exposure to currency risk where corporates realise income and pay expenses in different currencies.


Final chunk number 8, content : 
Managing these risks effectively is essential for ensuring financial health and meeting business goals. Traditional FX solutions are often bound either by market opening hours or requiring alternative arrangements which typically come with additional fees. Many systems still rely on legacy systems, including Real Time Gross Settlement (RTGS) and correspondent banking flows requiring downtime for maintenance. There have been strides made in traditional banking including the extension of trading hours and enhancements to RTGS networks, which, have helped reduce risk and enhance efficiency. Initiatives by bodies such as the FSB and CPMI aim to align RTGS operating hours and enhance infrastructure effectiveness.[1,2] Additionally, corporates frequently encounter challenges in understanding the full end-toend cost of executing cross-border payments. ## _**Potential use of tokenised bank liabilities**_ 

Tokenisation and shared ledger infrastructure have great potential to alleviate some of the pain points faced by market participants. Tokenisation refers to the process of converting a real-world asset, such as a traditional deposit, into a digital representation or "token" on a shared ledger.


Final chunk number 9, content : 
These digital tokens can act as secure, verifiable representations of the original asset.


Final chunk number 10, content : 
They can be transacted, transferred, or stored in a way that matches the functionality of traditional banking products, with enhanced efficiency, transparency, and security. Among the most relevant innovations enabled by tokenisation is the emergence of digital money (tokenised money) formats such as tokenised bank liabilities, stablecoins and central bank digital currencies (CBDCs)[ 3] , which are designed to function as stable store of value. > 1    https://www.fsb.org/wp-content/uploads/P211024-1.pdf 2    https://www.bis.org/cpmi/publ/d203.pdf 

> 3  Central bank digital currencies (CBDCs) and stablecoins are other forms of digital money with differences in overall design and governance in comparison to tokenised bank liabilities. CBDC is a direct liability of the issuing central bank, while private entities can issue stablecoins and are designed to maintain a constant value against one or more specified fiat currencies. On top of tokenised bank liabilities, the industry must carefully monitor the evolving landscape of stablecoins and CBDCs to navigate potential opportunities and challenges effectively. 01/07/2025 

5 

--- end of page.page_number=5 ---

This stability is critical for payments and settlement, where parity and redeemability into fiat currency remains essential during the transition towards broader recognition and acceptance of digital money. In the context of tokenised bank liabilities[4] used in transaction banking, tokenisation enables bank liabilities to be held and transferred as digital tokens, facilitating faster settlement and improving the overall accessibility of financial transactions. This paper will provide an overview of the potential applications of shared ledgers and tokenised bank liabilities used in transaction banking. In this respect, a common feature amongst many initiatives is a focus on use of the tokenisation of money and “smart contracts” to streamline the frequently complex process of FX payments and settlements.[5] 

Whilst recognising the development of various forms of digital money, including CBDCs and stablecoins, this paper will focus on the use of tokenised bank liabilities to facilitate payments and settlement in transaction banking. In this vein, this paper will: 

- focus on two pain points identified by workstream participants, namely: (i) implementing tokenised bank liabilities and shared ledger solutions in cross-border payments and FX settlements; and (ii) the lack of a generally accepted industrywide framework facilitating the adoption of tokenised bank liabilities; and 

- showcase Project Guardian use cases which have been designed to consider and/or address the pain points. In particular, these uses cases illustrate solutions that enable tokenised deposits in different currencies and issued by different deposit takers to be exchanged. While the use cases are in the experimental phase, these efforts nevertheless demonstrate that scalable and interoperable solutions can be developed in anticipation of tokenised assets market growth. This paper also expands on various design, operational and risk considerations in the use of tokenised bank liabilities and shared ledger in cross-border payments. This paper does not set out any positions on the legal classification or regulatory treatment of any tokenised bank liabilities which may vary by jurisdictions. > 4 Tokenised bank liabilities are tokens on a ledger that represent a commercial bank's liability, potentially including other forms of bank liabilities beyond deposits. > 5 This is one aspect discussed by the Bank for International Settlements (“ **BIS** ”) in _Blueprint for the future monetary system: improving the old, enabling the new_ https://www.bis.org/publ/arpdf/ar2023e3.htm. This paper focusses on private sector initiatives as opposed to the public sector, emphasising on tokenised deposits rather than initiatives which involve the input of central banks or governments, for example, central bank digital currencies (CBDCs). 01/07/2025 

6 

--- end of page.page_number=6 ---

## **2 Background** 

## **2.1 Cross-border payments and the FX market** 

Liquidity management is a cornerstone of a treasury function facilitated by transaction banking solutions, requiring the seamless movement of currencies to meet payment obligations and funding needs. Effective liquidity management facilitates domestic and cross-border payment obligations, the optimisation of cash flows, and, in turn, improves financial stability. Liquidity management often involves converting currencies and managing intra-day or short-term funding gaps to avoid disruptions in payments or settlements and prevention of a wider systemic risk.


Final chunk number 11, content : 
In times of market volatility, domestic and cross-border payments paired with FX transactions enabling liquidity solutions becomes even more critical, as they allow institutions to respond swiftly to changing conditions and maintain confidence in their operations. These activities not only address immediate operational needs but also contribute to the depth and resilience of the global FX market, thereby also maintaining price continuity and price stability in the FX markets. ## **2.2 Differences between traditional transactions and transactions using tokenised bank liabilities** 

The BIS has carried out extensive research on the use cases where tokenisation is easiest and where systemic gains are expected to be greatest.[6] The BIS notes that tokenisation can “dramatically enhance the capabilities of the monetary and financial system by harnessing new ways for intermediaries to interact in serving end users, removing the traditional separation of messaging, reconciliation and settlement”. In this respect, tokenised bank liabilities have the potential to alleviate some key pain points for transaction banking: 

_**Complexity of cross-border payments:**_ Payments in the transaction banking space are frequently cross-border, involving different currencies and time zones. Cross-border settlement requires different intermediaries and disparate payment systems across institutions and can be expensive, slow and opaque, reflecting multiple frictions.[7] Tokenised bank liabilities, as a programmable layer of money, have the potential to simplify crossborder payment processes by reducing reliance on intermediaries and enabling settlement over shared infrastructure/system. _**Dependency on multiple local RTGS clearing bound by cut-off times:**_ FX markets are only open five and a half days a week. The payment of these transactions is affected by time differences and payment systems operating hours. Meanwhile, shared ledger and tokenised 

> 6     https://www.bis.org/publ/arpdf/ar2023e3.htm 

> 7 - Financial Stability Board (FSB). 2020. “Enhancing Cross-Border Payments: Stage 3 Roadmap.” Basel, https://www.fsb.org/wp - content/uploads/P131020 1.pdf; Committee on Payments and Market Infrastructure (CPMI).


Final chunk number 12, content : 
2020. “Enhancing Cross-Border Payments: Building Blocks of a Global Roadmap,” Stage 2 report to the G20.


Final chunk number 13, content : 
Bank for International Settlements, Basel. https://www.bis.org/cpmi/publ/d193.pdf. 01/07/2025 

7 

--- end of page.page_number=7 ---

bank liabilities-based solutions offer 24/7 operations. Payment systems who use these solutions could potentially eliminate delays caused by varying cut-off times due to time difference, and non-operating markets on weekends and public holidays. _**Consequent Implications for FX settlement:**_ The complexity of cross-border payments and dependency on RTGS clearing consequently create downstream implications in the settlement of FX transactions. Two notable implications are the lack of instantaneous settlement (with standard market conventions being T+1/T+2 for most pairs) and the presence of settlement risks, with shared ledger being well-positioned to address the first implication as it is potentially available 24/7. On the second implication, CLS Group’s product, CLSSettlement provides an effective risk mitigation and multilateral netting solution.[8] However, only payments in 18 currencies[9] are currently supported. A recent estimate[10] suggested that 10%-15% of trades by value are settled on a gross bilateral basis without risk mitigation which is likely due to either the currency or counterparty not being CLS eligible. Shared ledger and tokenised bank liabilities coupled with adequate operational and legal frameworks have the potential to mitigate this risk, enabling atomic settlement for a wide range of currencies with commonly recognised settlement finality. _**Fragmented settlement layers:**_ Operational friction arises from distinct settlement layers, each managed by different parties with independent workflows. Implementing shared ledger can simplify this by integrating settlement into a single process and technology layer, reducing intermediary reliance, and enhancing transaction visibility in the long run. That said, meaningful implementation will require a sufficient number of participants to commit to the development and adoption of interoperable solutions that are consistent with their internal governance, compliance, and control requirements. - _**Cost of cross border payment**_ : In cross-border payments, a common settlement asset or common settlement platform does not exist. The use of multiple intermediaries in traditional cross-border payment adds to transaction costs, notably for retail payments, despite G20 efforts to reduce them[11] . The lack of a common settlement asset requires banks to either extend credit to each other or to pre-fund potential cross-border payment needs. The funds locked in these pre-funding accounts represent opportunity costs, which may ultimately be passed on in the form of fees.


Final chunk number 14, content : 
Interoperable tokenised bank liabilities used to complete settlement may reduce the need for multiple intermediaries and the associated costs. There are also potential indirect savings from improved operational efficiencies through the deployment of smart contracts, though it should be noted that it may be offset by the cost of initial setup and implementation. > 8  https://www.cls-group.com/products/settlement/clssettlement/ 

> 9 At the time of this paper, CLS supports 18 currencies and settles 94% of average daily FX volumes on a PvP basis. > 10  https://www.bankofengland.co.uk/speech/2024/december/philippe-lintern-speech-at-fx-markets-europe-on-global-fx-code 

> 11 The FSB has studied the cost of cross-border payments. For retail cross-border payments, the FSB’s target is to bring the global average cost of payment to be no more than 1%, with no payment corridors with costs higher than 3% by end-2027. The FSB notes that the average costs for B2B cross-border payment transactions in 2024 is still 1.6%; the average cost of B2P and P2B cross-border payment transactions is 2% and that for P2P transactions is higher at 2.6%. FSB _Annual Progress Report on Meeting the Targets for Cross-border Payments_ , _2024 Report on Key Performance Indicators_ https://www.fsb.org/uploads/P211024-3.pdf 

01/07/2025 

8 

--- end of page.page_number=8 ---

_**Capital controls affecting movement of domestic currency:**_ Select countries implement currency-based Capital Flow Management Measures (e.g., CFMM) to manage and regulate financial flows. The manner in which capital controls are currently implemented can affect the access to funds and efficiency of cross-border settlement when involving affected currencies. Tokenisation allows for the coupling of programmability with value transfer (in the form of digital money). This can enable the embedding of capital control policies (e.g., CFMM as noted in BISIH's Project Mandala or CNY/CNH rate curves) in the transfer of digital money for cross-border payments, promoting greater efficiency, transparency and potentially greater compliance adherence. 01/07/2025 

9 

--- end of page.page_number=9 ---

## **3 Implementation of shared ledger and tokenised bank liabilities in transaction banking** 

## **3.1 Key lifecycle stages of traditional cross-border payments** 

This section explores how tokenised bank liabilities can enhance workflow efficiency and interoperability compared to the existing operating model. While real-world implementation involves significant complexity and dependencies, this high-level perspective provides a foundational benchmark for assessing the evolution of payments. Additionally, we explore the key building blocks within operating models that support gradual integration, ensuring that tokenised bank liabilities can effectively function in transaction banking. ## **3.1.1 Traditional Lifecycle of Cross-Border Payments** 

**==> picture [403 x 175] intentionally omitted <==**

_Figure 1: Illustration of a traditional lifecycle of cross-border payments_ 

The figure above illustrates a simplified traditional lifecycle of cross-border cross-currency payments, facilitated by a correspondent bank. For instance, Corporate A is making a payment to Corporate B, who is in country B and receiving in another currency (i.e., Currency B). Such cross-border cross-currency payments are common in international trade, manufacturing and supply chain management, cross-border asset management, inter-subsidiary/group payments, etc.


Final chunk number 15, content : 
In this case, 

1. First, Corporate A (i.e., Sender) initiates a payment request and FX transaction with its bank (e.g., Bank A) by providing details of the recipient (e.g., bank account and purpose of payment). 2. Bank A (i.e., Sender’s Bank) receives the payment request and validates the payment order ensuring that there are sufficient funds and required documentation prior to sending SWIFT message (MT103) to Bank B (i.e., the Receiver’s Bank). Bank A also conducts the necessary Anti-Money Laundering (AML) and Countering Financing of Terrorism (CFT) checks. 01/07/2025 

10 

--- end of page.page_number=10 ---

3. In the case where there is no correspondent banking relationship between Bank A and Bank B, an intermediary bank may be engaged. The intermediary bank may be servicing the FX for this transaction. In this case, the intermediary bank would receive a SWIFT message (e.g., MT202 or MT202 COV) to facilitate funds movement between banks. The intermediary bank would similarly check on compliance, ensures liquidity and processes the funds through its own channels (e.g., nostro/vostro with Bank A and Bank B accordingly). 4. Bank B (i.e., Receiving bank) receives the SWIFT message (e.g., MT103 message) and verifies the payment details (e.g., Corporate B’s account details, amount to be received). Bank B also conducts the necessary AML and CFT checks as the receiving bank. 5. Payment is processed by Bank B and credited to Corporate B's account. Corporate B is notified that funds are received and can be deployed to other use. The funds are generally settled across the banks through correspondent banking between banks, or independent multi-currency settlement systems: 

1. With correspondent banking arrangement, the end-to-end payment and settlement time may increase while transparency would be reduced when the payment chain increases in complexity and see greater number of intermediary bank involvement. Further, the cut-off times of RTGS systems and time difference across time zones would affect the time required for settlement after the initial payment initiation. 2. Independent multi-currency settlement systems are infrastructures with settlement capabilities in several currencies[12] .


Final chunk number 16, content : 
These systems allow for payments versus payments (PvP), addressing settlement risk in cross-currency payments. However, some of these systems may only support a specific set of currencies, and costs and operational complexity[13] have been cited as other potential challenges to adopt such systems. ## **3.1.2 Innovating Cross-Border Payments with Tokenised Bank Liabilities** 

Tokenised bank liabilities can potentially facilitate more efficient transaction banking services, which could be faster, cheaper and have greater transparency. There are different approaches to which tokenised bank liabilities may be implemented at the token level and ledger level. _**1. Token Level: Tokenised bank liabilities could take the form of a digital twin tokens or a digitally native token.**_ 

   - a) _Digital twin tokens_ 

> 12  Bech, M, U Faruqui and T Shirakami (2020): “Payments without borders”, BIS Quarterly Review, March, www.bis.org/publ/qtrpdf/r_qt2003h.pdf 

> 13  Settlement risk in foreign exchange markets and CLS Bank. BIS Quarterly Review, December 2002 https://www.bis.org/publ/qtrpdf/r_qt0212f.pdf 

01/07/2025 

11 

--- end of page.page_number=11 ---

A form of tokenised asset that is issued and custodied traditionally, but also converted onto a shared ledger network through digital twin tokens that convey ownership interests in the underlying traditional assets (i.e., a bank liability), representing a claim against the issuing institutions. The tokenisation process involves converting ownership and rights of the traditional bank liability into a digital token that can be more easily and efficiently transferred, settled and managed over shared ledger-based systems. Banks’ tokenised bank liabilities is an example of digital twin tokens where the depository institution that issued the token is liable to the owner of the underlying off-chain liability, with the transferrable ownership rights represented on-chain by the token[14] . In the transaction banking context, the nature of this form of digital tokens makes it a suitable medium for payments and settlement. ## b) _Digital native tokens_ 

A reference to assets that are issued and custodied natively on a shared ledger only (which constitutes the golden source of truth in relation to ownership rights), and therefore do not have traditional assets as an underlying basis[15] . Digital native tokens are legally recognised as existing on-chain only, with shared ledger networks serving as official asset registers, and functionality encoded via smart contracts allowing for automated and transparent behaviour. _**2. Ledger Level: Tokenised bank liabilities’ transactions on a shared ledger can be executed in different ways: within a single bank’s group ledger, between two banks’ ledgers or across interlinking distinct networks.**_ 

## _a) Within a single bank[16] ledger_ 

A single bank may establish a shared ledger with its entities or branches hosting the nodes. Participants may join this common shared ledger and transact bilaterally or with multiple counterparties within this network, with transactions being atomically settled on a delivery-versus-payment (DvP) or payment-versus-payment (PvP). In such an implementation, corporates can leverage tokenised bank liabilities to optimise their liquidity management by freely moving multi-currency deposits across their subsidiaries in multiple jurisdictions. Tokenised bank liabilities are designed to enable real time, 24/7 fund transfers and deployment in a single bank’s group ledger allow cross-border payments to be processed as “on-us” transactions. While these are existing capabilities in traditional banking today, a single bank ledger remains a crucial foundational piece for various deployment models involving tokenised bank liabilities and potentially interoperability with other forms of digital money. This design also generally offers banks and corporates comfort in terms of risk management, security and safety implication. > 14  Project Guardian Open interoperable network, https://www.mas.gov.sg/publications/monographs-or-informationpaper/2023/project-guardian-open-interoperable-networks 

> 15  Ibid. > 16   Single bank ledger in this context, refers to an internal shared ledger operated within the same banking group, enabling intra-group transfers between accounts held across different legal entities or branches. Such transfers can be effected cross-border, subject to the geographic and regulatory coverage of the banking group. 01/07/2025 

12 

--- end of page.page_number=12 ---

Furthermore, the digital nature of tokenised bank liabilities enables programmability, allowing different conditions (e.g., rule-based target balances and capital flow measures) to be embedded. This introduces greater flexibility and customisation for corporates in liquidity management while also potentially incorporating compliance with jurisdiction-specific policies. For instance, corporates can determine the target balances across various entities in different jurisdictions across various currencies. Corporates need not follow fixed rules or fields in determining target balances and specific cut-off times. ## _b) Between banks_ 

Due to the nature of international trade, corporates may need to make payments to suppliers/merchants that bank with different banks. To facilitate such payments, banks may choose to leverage their respective tokenised bank liabilities across their own shared ledger networks. This would serve as an alternative to existing correspondent banking. From the corporates’ perspective, tokenised bank liabilities deployed across banks’ shared ledger can similarly be executed on a 24/7 basis, and the ability to support PvP, combined with the atomic nature of the transactions, enhances settlement certainty and mitigates settlement risk. PvP could be orchestrated through varying technical implementations such as escrow smart contracts, cross-chain swaps or protocols. While it may be technically feasible, this approach will require further exploration. Depending on the implementation, it may require financial institutions to be comfortable holding the counterparty’s tokenised bank liabilities on the counterparty’s ledger and there may be an element of prefunding if its implementation emulates existing correspondent banking models. Prior to PvP execution between two different banks’ shared ledger, it has been observed that banks may leverage cross-chain implementation for the exchanges of messages (e.g., through cross-chain mechanism such as Hashed Timelock Contracts (HTLC)) as an interim to better understand the risks, considerations and controls required for cross-chain interactions for value transfer. ## _c) Across interlinking distinct networks[17]_ 

There are multiple efforts internationally exploring a network of interconnected banks transacting with tokenised monies. These interlinked networks can consist of a network of independent or layered networks, application-specific chains, or sidechains, each with their own distinct governance framework and other customisations to facilitate clearing and settlement of one or more currencies. Depending on the design and the governance of the network(s), banks can deploy tokenised (cash and non-cash) assets and facilitate transactions on a PvP and/or 

> 17  Project Guardian Interlinking Networks Technical Paper (2023), https://www.mas.gov.sg/publications/monographs-or-informationpaper/2023/interlinking-networks 

01/07/2025 

13 

--- end of page.page_number=13 ---

DvP basis. As corporates generally diversify their risks and do not rely on a single bank, such network(s) could serve as an international settlement network that better facilitates multi-bank transactions and addresses liquidity fragmentation. Cross-border payments using tokenised bank liabilities can be implemented through various models, which generally fall into a few broad categories. The table below summarises these potential implementation models. |Implementation<br>Model|Interoperability<br>protocol required|Currency<br>conversion required|Key Considerations|
|---|---|---|---|
|Single Bank Shared<br>Ledger Network<br>(Single Currency)|No|No|Operates within a<br>single bank’s shared<br>ledger to enable<br>internal real-time<br>transfers.|
|Multi-Bank (Single<br>Currency, Same<br>Shared Ledger<br>Network)|Partial18|No|Participating banks<br>operate on a<br>shared<br>permissioned<br>ledger with needed<br>agreement on<br>access, rules, and<br>settlement.|
|Multi-Bank (Single<br>Currency, Different<br>Shared Ledger<br>Networks)|Yes|No|Participating banks<br>operate on<br>separate ledgers.<br>Interoperability<br>protocols would be<br>required to<br>coordinate<br>messaging and<br>settlement.|
|Cross-Currency<br>(Single or Multi-<br>bank)|Depending on the<br>Shared Ledger<br>network and<br>tokenised deposit<br>pair involved.|Yes|FX mechanism is<br>required to support<br>cross-currency<br>transactions,<br>operating within a<br>single shared ledger<br>network or across<br>networks.|



_Table 1: Potential implementation models for cross-border payments using tokenised bank liabilities_ 

> 18  In a scenario where a bank adopts another bank’s shared ledger network, some integration and connectivity may be required to ensure alignment with the host network’s governance and technical standards. 01/07/2025 

14 

--- end of page.page_number=14 ---

## **3.2 Design Considerations** 

## **3.2.1 Building towards interoperable tokenised bank liabilities transactions** 

A real-world application of cross-border payments with tokenised bank liabilities would likely face a scenario where the tokenised bank liabilities are denominated in different currencies and issued on different shared ledger networks by different banks. This raises the question as to how to ensure interoperability of tokens across different shared ledgers. Though this is analogous with the traditional model where there are dependencies on correspondent banking relationships or the involvement of intermediary banks, designing operating models on the shared ledger network allows for an opportunity to reimagine the messaging, clearing and settlement processes between financial institutions.


Final chunk number 17, content : 
Interoperability remains a fundamental prerequisite for the wider adoption of tokenised bank liabilities-based transactions and continues to be a focus area for financial institutions and market participants’ resources. In one approach, further explored in this paper under Use Case 1, a liquidity provider is in effect the new intermediary and swaps tokens from one currency to another. The paying bank only needs to participate on one shared ledger network and interface with the liquidity provider to connect to other shared ledger networks and complete the payment. Standardised messaging on both shared ledger networks will be needed to ensure all payment requests capture the same information and can be completed. In another approach showcased in Use Case 2, payments performed on two private and permissioned shared ledger networks explored using Hash Time-Locked Contracts to establish a bilateral exchange of either messages or tokens across the two chains. This method is able to streamline both banks’ operations and reliance on a single coordination point. ## **3.2.2 Price quotations for cross-currency transactions** 

Another key consideration in the interoperability of different currency tokenised bank liabilities would be the sourcing of price quotations for cross-currency transactions, both within the same financial institution’s shared ledger network and between financial institutions. To provide timely and accurate price quotations for FX conversion rates, network operators should consider how established market best practices can be incorporated into the shared ledger ecosystem. In the early stages of a shared ledger network, on-chain trading volumes, when implemented, will not be sufficient to generate reliable market FX rates purely from onchain data. Therefore, an automated method, such as a price oracle, will be needed to source and incorporate reliable market FX data with minimal latency. 01/07/2025 

15 

--- end of page.page_number=15 ---

To ensure a robust and representative data sample while minimising the likelihood of outliers, it is recommended to aggregate multiple institutional grade data sources using algorithmic computations. Potential sources of live market data could include central bank benchmark rates, internal bank liquidity desks, and market data providers like Bloomberg and Reuters. Due to the computationally intensive nature of these algorithms, it may be more efficient to execute the calculations off-chain.


Final chunk number 18, content : 
Once the data sources have been aggregated into a single benchmark rate for each currency pair, they can then be integrated into the shared ledger network via an oracle service. The oracle can be fed by APIs at regular high frequency intervals to ensure secure and efficient data transmission. Maintaining data integrity and preventing any type of manipulation should be a top priority to ensure confidence in the network. Data fed to the shared ledger network should be updated with minimum viable time latency as prices are updated on existing electronic platforms every two microseconds and incorporate functionality to check for ‘stale’ prices. Anti-manipulation measures could be implemented via cryptographic tools like basic hash functions to prove that the data has not been tampered with without revealing any sensitive information. At some point in the future, the shared ledger ecosystem will have developed to a point where on-chain price formation and execution are possible such that there will be less reliance on existing venues used for discovery.


Final chunk number 19, content : 
The on-chain FX data can be made visible to all users in a way that obfuscates private data of individual users and transactions while using advanced cryptographic tools such as zero-knowledge proofs to demonstrate the data’s validity. The aggregated public market data will be available simultaneously to all users and could prevent rent-seeking behaviour that can result from information asymmetries. ## **3.2.3 Design principles for tokenised bank liabilities in transaction banking** 

To promote interoperability across different product frameworks and operating models for tokenised assets, a set of standardised design principles for tokenised fixed income products was developed under the Guardian Fixed Income Framework.[19] The design principles are technology and jurisdiction-agnostic. Some of these design principles also apply to tokenised bank liabilities and relate to smart contracts insofar as they are used to create the tokens to which tokenised bank liabilities relate. These are as summarised below: 

|**#**|**Principle**|**Description**|
|---|---|---|
|1|Valid existence of<br>the issuer|At the time of issuance and during the life of the tokenised<br>bank liabilities, the issuer of the tokenised bank liabilities,<br>which is typically a financial institution, is duly established and<br>validly existing under the law under which it is organised.|



> 19 Project Guardian: Fixed income Framework, November 2024 

01/07/2025 

16 

--- end of page.page_number=16 ---

|**#**|**Principle**|**Description**|
|---|---|---|
|2|Validity<br>of<br>the<br>tokenised<br>bank<br>liabilities|The tokenised bank liabilities are duly issued and constitute<br>legal, valid and binding obligations of the issuer. The tokenised<br>bank liabilities and the related legal rights should be capable of<br>being attached with, or themselves constitute a digital token<br>recorded on a ledger20, consistent with the governing law of<br>the jurisdiction in which the tokenised bank liabilities are<br>recognised. In the case of digital twin tokens, the tokenised<br>bank liabilities and the underlying legal rights are inextricably<br>linked – meaning that the token cannot be transferred<br>independently of the legal rights and vice versa.21|
|3|Authorisations<br>and consents|The issuer and any intermediaries or service providers should<br>have obtained and will maintain all relevant authorisations and<br>consents, including any licenses, from any relevant supervisory<br>or regulatory authority.|
|4|Tokenisation<br>process|The process of tokenisation, including the use of shared ledger<br>and smart contracts, and the maintenance of any records<br>relating to the tokenised bank liabilities, should not alter or<br>affect the terms of the transaction documentation. If any<br>inconsistency arises, appropriate measures should be taken to<br>ensure continued alignment with the agreed legal terms and<br>compliance with all applicable laws. Any restrictions on<br>transferability should be embedded in the functionalities of<br>the smart contract or be compatible with the technology<br>platform.|
|5|The ledger|The applicable ledger should comply with any applicable law<br>and have regard to any applicable principles, standards and<br>best practices developed and recognised by industry bodies,<br>trade associations, or as commonly adopted in the market. The<br>ledger should be able to accommodate disposals and transfers<br>of the tokenised bank liabilities and be public and transparent.<br>The ledger should have integrity and be fit for purpose.|
|6|Minimum<br>features of smart<br>contracts|The minimum features of smart contracts should address the<br>governance of the smart contract and should use technology<br>and risk frameworks to mitigate smart contract risks.|
|7|Tokenisation<br>terms|The transaction documentation should take into account and<br>be consistent with the governing law of the tokenised bank<br>liabilities. It should clearly set out the rights of holder and|



> 20  These design principles use the generic term "ledger" to refer to ledgers built using shared ledger, whether such ledgers are centralised or decentralised and permissioned or permissionless. The term ledger should be read accordingly. > 21  Whether such linkage is enforceable will depend on the legal recognition and treatment of tokenised representations in the relevant jurisdiction. 01/07/2025 

17 

--- end of page.page_number=17 ---

|**#**|**Principle**|**Description**|
|---|---|---|
|||disclose any associated risks. The transaction documentation<br>should establish a transparent and fair process for addressing<br>loss or theft of a private key and managing the consequences<br>of token cancellation. There should be clearly defined<br>procedures for detecting and responding to any breaks in<br>reconciliation, including the investigation measures and<br>related communication. At the same time, the documentation<br>should reflect the diversity of tokenised products and variance<br>in their legal and technical features.|
|8|Information<br>on<br>the functioning of<br>the ledger and<br>smart contract|The issuer should make available to that each holder of the<br>tokenised bank liabilities all material information regarding the<br>functioning of the ledger and the smart contract used for the<br>tokenisation process. This includes clear and accessible<br>information on the technical and organisational measures<br>implemented to protect the functioning, integrity and security<br>of the ledger and the smart contract.|
|9|Risk management|There should be a rigorous governance framework and<br>effective control mechanisms to counter cyber security risks<br>and data protection principles.


Final chunk number 20, content : 
Smart contracts should be<br>extensively tested before deployment using various scenarios<br>and stress tests. The ledger and smart contracts should<br>implement strong access controls to ensure only permitted<br>persons can modify or interact with the ledger/smart<br>contracts. Human intervention should be integrated into the<br>workflows at critical points for added security.|



_Table 2: Design principles for tokenised bank liabilities_ 

01/07/2025 

18 

--- end of page.page_number=18 ---

## **4 Operating models and considerations** 

## **4.1 Changes in operating practices** 

As the operating models evolve, this in turn will necessitate changes to existing operational practices: 

## _**Adoption of Tokenised Bank Liabilities in Transaction Banking**_ 

Payment processes shift with the adoption of tokenised bank liabilities, moving away from traditional payment methods involving the debiting and crediting of bank accounts and may rely on on-chain wallet addresses for settlements. While shared ledger-based models offer potential efficiency gains and reduced reliance on intermediaries, certain constraints remain. Time zone mismatches remain a key hurdle, requiring coordination between financial institutions operating in different settlement timetables.


Final chunk number 21, content : 
For example, today’s fiat currency FX spot settlement generally occurs on a T+2 basis to account for time zones, the differing operating hours of domestic RTGS systems and the need to coordinate the activities of correspondent banking relationships. T0 settlement is possible for currencies where the time-zone and RTGS systems are operating. The use of tokenised bank liabilities does not, at least at these initial stages, resolve these time zone issues and the need to prefund. Co-existence of emerging shared ledger-based platforms with legacy systems is expected to persist for some time. This overlap means that banks and financial institutions must maintain hybrid models that integrate tokenised bank liabilities with traditional infrastructure to allow co-existence and scalability. These hybrid models will be instrumental in ensuring institutions can leverage the efficiencies of shared ledgers while retaining interoperability with legacy fiat-based systems during the adoption phase. Ultimately, as familiarity and sophistication improve owing to wider adoption over time, on-and-off-chain ramps (fiat-to-token exchanges) are currently relied upon as the operating models for the co-existence of fiat deposits and digital money (e.g., tokenised bank liabilities). These ramps will enable institutions to move funds seamlessly between shared ledgers and legacy systems. They also supplement critical workflows related to token exchanges into fiat, reconciliation between off-chain and on-chain balances, ensuring integration with RTGS systems to support compliance and alignment with existing payment standards. As shared ledgers gradually matures in its adoption, the operating systems for the interaction between fiat deposit and digital money would likely evolve over time. ## _**Clarity on the Recognition of Tokenised Bank Liabilities in Different Jurisdictions**_ 

Effecting cross-border settlements directly to wallets maintained in different jurisdictions will require a degree of legal alignment or mutual recognition regarding the legal nature of tokenised bank liabilities across these jurisdictions. At this point, existing case law on virtual assets remains limited, and the legal characterisation of tokenised bank liabilities does not appear to be settled. Additionally, there are concerns that the absence of formal legal recognition of tokenised bank liabilities in some jurisdictions may affect their function as a stable store of value and limit the availability of suitable token-issuing deposit banks, 

01/07/2025 

19 

--- end of page.page_number=19 ---

particularly for emerging market currencies. As developments in this area continue to evolve, the legal nature of tokenised bank liabilities may vary across jurisdictions, with different countries taking diverse approaches based on their financial regulatory frameworks, legal traditions (common law vs civil law), and progress in digital asset regulation. ## _**Integration of Shared Ledger in Accounting Practices**_ 

Wider use of tokenised bank liabilities will require accounting standards and banking general ledgers (GLs) to accommodate the recognition, measurement and reporting of realtime shared ledger-based payments and balances. Additionally, there may be challenges in handling tokenised bank liabilities with additional functionalities attributed to the programmability of such tokens (e.g., embedded smart contract logic, automated checks or interest accrual features). Liquidity fragmentation also further complicates accounting, as institutions must reconcile fiat and token balances across time zones to ensure operational smoothness. Operational setups will therefore include automated reconciliation mechanisms to match digital and fiat balances, while minimising inefficiencies and opportunity costs associated with idle tokenised liquidity. ## _**Evolving Role of Correspondent Banks**_ 

As tokenised payments become more widespread, the role of correspondent banks may also change as it may lead to reduced intermediation between institutions. However, correspondent banks are uniquely equipped to bridge shared ledger networks with legacy systems by facilitating fiat-to-token exchanges, providing deposit account services, and offering credit solutions. Additionally, correspondent banks could further enable shared ledger adoption by participating in multi-participant networks (such as shared ledger PvP Orchestration networks) or act as liquidity providers to facilitate the intermediation between token issuers, currencies and shared ledger networks. ## _**Currency Conversion Practices**_ 

The transition from direct fiat currency conversion to exchanges between tokens denominated in various currencies may require adapting operating standards particularly for sourcing and application of currency reference. At the initial stages of adoption, financial institutions would need to depend on off-chain external benchmarks integrated through automated systems and on the shared ledgers via price oracles. In the long term, as the shared ledger ecosystem meets key prerequisites, it is possible that financial institutions could transition to native on-chain FX rate discovery, leveraging aggregated transaction data to determine pricing. ## _**Standardisation of Smart Contracts**_ 

For payments made across shared ledgers or financial institutions, standardised specifications will be needed to ensure the interoperability of payment requests across different shared ledger networks. 01/07/2025 

20 

--- end of page.page_number=20 ---

## _**On-Chain Settlement Standards**_ 

If in due course settlement becomes integrated, standardised processes will have to be established for settlement directly on the shared ledger. These standards will address critical requirements, such as settlement finality, interoperability, and legal certainty, to ensure reliability in payment flows. Settlement finality refers to the legally defined point in time when a payment or transfer becomes final, unconditional, and irrevocable.


Final chunk number 22, content : 
It is essential to prevent risks such as clawbacks due to insolvency, which could undermine the stability of the payment system. Alongside operational rules, the laws of relevant jurisdictions must be reviewed to confirm that legal principles governing settlement finality extend to shared ledger-based payments, such as those involving tokenised commercial bank deposits. Where necessary, legal opinions should be obtained to clearly define the point at which finality occurs. In many jurisdictions, specific legislation has already been enacted to ensure settlement finality in traditional systems, but further adaptation may be needed for shared ledger-based models. The development of shared ledger-based settlement frameworks, particularly for crossborder transactions, will require careful legal reviews to ensure settlement finality is upheld across diverse jurisdictions. In some cases, transaction banking involving tokenised bank liabilities may continue to operate within existing RTGS systems. For these implementations, legal reviews might conclude that settlement finality is already supported under current laws. However, this may not be the case for newer payment models which rely entirely on decentralised networks. Establishing proper scale requires standardisation across the industry. The Global Layer One (GL1) initiative[22] exemplifies this by developing an ecosystem of market infrastructures that aligns with regulatory requirements. It focuses on creating common standards for governance, risk management controls, and settlement arrangements for cross-border transactions. GL1’s primary goal is to establish financial market infrastructure standards and specifications that will govern how GL1-compliant platforms operate. This framework enables institutions to validate their services against internationally recognised principles and meet regulatory requirements across different jurisdictions. GL1 specifically details the necessary controls for financial market infrastructures running shared ledger infrastructure, while providing guidance on addressing compliance gaps to meet these standards. ## _**Regulatory and Compliance Adaptations**_ 

Anti-Money Laundering (AML) checks and transaction monitoring will remain essential to enable banks to comply with applicable banking requirements albeit that these will have to be integrated to the on-chain environment (at the pre-trade, trade and post-trade stage). Compliance frameworks will evolve to leverage the shared ledger’s inherent transparency, and financial institutions to streamline AML/KYC obligations while maintaining regulatory rigor. While shared ledger can enhance on-chain monitoring and validation processes, tokenised bank liabilities must continue to comply with off-chain checks, such as verifying 

> 22    Global Layer 1 (MAS) (2024). https://www.mas.gov.sg/publications/monographs-or-information-paper/2024/gl1-whitepaper 

01/07/2025 

21 

--- end of page.page_number=21 ---

the source of funds, in accordance with established anti-money laundering laws and global regulatory standards. Operational practices will progressively utilise the shared ledger’s auditability and immutability to supplement traditional compliance methods. These hybrid frameworks will balance the efficiency gains enabled by shared ledger technology with the risk management requirements of regulatory oversight. By integrating regulatory workflows into tokenised systems, financial institutions can achieve a seamless merger of innovation and compliance, safeguarding trust and integrity within the financial ecosystem. GL1’s Programmable Compliance Toolkit[23] , demonstrates how jurisdiction-specific regulatory requirements, including AML checks and capital flow management measures, can be encoded as conditions within programmable wrappers that hold tokenised assets. This approach enables real-time verification of compliance requirements, exemplifying regulatory oversight for transactions utilising shared ledger infrastructure. ## _**Global Settlement Date and Point of Settlement**_ 

The concept of a “global trading date” (i.e. a globally consistent definition of a singular synchronised trading date) is of significant value in mitigating settlement risk, increasing the opportunity for PvP, and is expected to positively impact how counterparty risk exposure is measured and mitigated. It may also result in other ancillary benefits such as creating a framework for the development of an intraday FX swap market. However, the shift towards 24/7 payment capability may require the concept of “Settlement Date” to be re-considered. There is potential to align the “Settlement Date” concept with the concept of a “global trading date”. A “global settlement date” will require careful consideration given the significantly wider implications to traditional financial instruments, such as FX, interest rates and other asset classes. Needless to say, there will also be profound impact on downstream products (e.g., derivatives) and their associated processes. Closely linked to this is the point of settlement, where the settlement of an FX transaction is often determined by the reconciliation of end-of-day agent bank statements to confirm receipt of funds, i.e., confirms settlement finality has occurred. Transitioning to a framework based on a “global settlement date” would transform how settlement finality is confirmed and processed. An extended, globally consistent timeline would necessitate automated reconciliation mechanisms such as the use of timestamps to ensure timely confirmation of settlement across jurisdictions. A timestamp could be an important technical development in the management of real-time payments, allowing real-time credit management and ultimately freeing up funds for other purposes, such as investment. > 23 Global Layer 1 (GL1) (2024), Programmable Compliance Toolkit. https://doc.global-layer-one.org/docs/programmablecompliance/overview/introduction. 01/07/2025 

22 

--- end of page.page_number=22 ---

## _**Set-Up Costs, Operational Challenges, and Capital Considerations**_ 

At the initial stages, participants must account for setup costs related to system connectivity and account structures. Separate balances will also need to be maintained across fiat and digital accounts, leading to increased funding costs.


Final chunk number 23, content : 
In certain cases, the need to ensure settlement across multiple time zones may require pre-funding accounts ahead of time, resulting in excess balances in these accounts.


Final chunk number 24, content : 
These balances typically do not earn interest, leading to opportunity costs that could impact FX rates. While these challenges present initial hurdles, they are likely to be addressed as digital money and tokenised bank liabilities mature. Furthermore, deposit takers must carefully evaluate the capital and liquidity impacts of issuing tokens backed by bank liabilities. The conversion of bank liabilities into tokens can affect liquidity coverage ratios (LCR), as such bank liabilities may need to be allocated to low-risk liquid assets, such as high-quality liquid assets (HQLAs). This reduces the deposit takers’ effectiveness in supporting economic financing and maturity transformation compared to traditional bank liabilities. ## _**Liquidity Providers Operational and Governance Considerations**_ 

Liquidity providers may face several challenges in supporting tokenised payments for settlements, including ensuring sufficient liquidity in applicable tokens prior to transactions and developing processes for minting and burning tokens to manage liquidity pools. Legal arrangements between providers and banks remain untested, necessitating the creation of standardised contracts. In multi-provider scenarios, criteria for provider selection, governance standards, and selection mechanisms will need to be defined to ensure effective management and transparency. ## **4.2 Potential operating models** 

Tokenised bank liabilities in different currencies and issued by different deposit takers need a way to be exchanged to ensure the transfer of funds from one country to another can be completed end-to-end. The case studies considered in more detail in Section 6 illustrate the various solutions that have been devised to achieve this, though these are mainly at an experimental stage. In summary these are: 

- the use of a liquidity provider to swap between tokens in different currencies issued by different issuers (Use Case 1); 

- the use of “Hash-Time Locked Contracts” to create interoperability between two private permissioned shared ledger networks (Use Case 2). In this model, tokens are not exchanged but funds are held in escrow and are released at a certain defined point using smart contracts; and 

- the use of shared ledger-based PvP Orchestration – OSTTRA, an industry leading shared ledger platform which operates to facilitate the PvP settlement by participants of obligations arising under their bilateral FX transactions (Use Case 3). 01/07/2025 

23 

--- end of page.page_number=23 ---

## **5 Risk considerations and mitigants** 

As adoption of shared ledger-based payments and settlement gain traction, they introduce a new range of risks across the lifecycle—from adoption to operational maturity. While the design principles outlined in Section 3 provide a robust baseline for managing these risks, practical considerations must address real-world implementation and system evolution. ## **5.1 Risk Considerations** 

|**Area**|**Sub-Area**|**Risk Considerations**|**Potential Solutions**|
|---|---|---|---|
|**Strategic &**<br>**Market Risks**|Operational<br>Transition<br>Issues|Running parallel digital and<br>fiat systems introduces<br>complexity, with potential<br>reconciliation mismatches.<br>Fragmented liquidity may<br>impair FX execution.|Invest in seamless<br>integration between<br>legacy systems and<br>tokenised solutions to<br>reduce friction and<br>collaborate with liquidity<br>providers to mitigate<br>fragmentation.|
||Liquidity Risk|Low liquidity in non-major<br>currency pairs could affect<br>FX execution and<br>settlement.|Enhance liquidity pools<br>and partner with key<br>liquidity providers to<br>mitigate fragmentation in<br>FX markets, ensuring<br>smoother execution even<br>for smaller or less traded<br>currency pairs.|
|**Technological**<br>**Risks**|Smart Contract<br>Risks|Smart contracts may<br>malfunction, have bugs, or<br>behave unpredictably, and<br>immutability may prevent<br>reversal of erroneous<br>transactions.|Prioritise comprehensive<br>smart contract audits,<br>continuous testing across<br>various scenarios, and<br>monitoring to identify and<br>correct errors swiftly.|
||Cybersecurity<br>Risks|Risks of malicious actors<br>exploiting system<br>vulnerabilities due to<br>unpatched flaws or<br>cryptographic<br>breakthroughs.|Implement robust<br>cybersecurity measures,<br>regular vulnerability<br>assessments, and<br>adherence to best<br>practices in cryptography<br>for optimal protection.|
||Data Integrity<br>Risks|Ledger forks,<br>malfunctioning nodes, or<br>errors in the codebase may<br>compromise the integrity<br>of transaction records.|Adopt reliable, well-<br>maintained ledger<br>solutions and implement<br>strong data verification<br>processes to ensure|



01/07/2025 

24 

--- end of page.page_number=24 ---

|**Area**|**Sub-Area**|**Risk Considerations**|**Potential Solutions**|
|---|---|---|---|
||||accuracy and integrity in<br>all transactions.|
|**Operational**<br>**Risks**|Payment &<br>Settlement<br>Mechanisms|Complexities arise when<br>converting tokenised<br>assets to fiat, and systemic<br>risks increase with delayed<br>finality.|Integrate tokenised<br>payment rails with<br>existing financial systems<br>and conduct rigorous<br>testing for potential<br>failure points to ensure<br>smooth settlement<br>processes.|
||Interoperability<br>Risks|Risk of fragmentation or<br>delays if platforms and<br>protocols cannot reliably<br>interact.|Prioritise building<br>interoperable platforms<br>that can reliably<br>communicate across<br>different protocols and<br>payment networks,<br>reducing potential delays<br>in cross-platform<br>transactions.|
||Third-Party<br>Providers Risks|Dependencies on third-<br>party service providers<br>(e.g., cloud services) may<br>introduce risks of failure or<br>latency.|Ensure thorough due<br>diligence and have<br>contingency plans in place<br>for third-party risks,<br>evaluating providers for<br>reliability and scalability.|
|**Legal &**<br>**Regulatory**<br>**Risks**|Jurisdictional<br>Variability|Tokenised instruments<br>may not be uniformly<br>recognised under law,<br>particularly in cross-border<br>FX use cases.|Engage with regulatory<br>bodies early to influence<br>the development of clear<br>legal frameworks,<br>ensuring compliance and<br>helping shape policy.|
||Evolving<br>Regulations|Shifting regulatory<br>landscapes could affect the<br>legal enforceability, tax<br>treatment, and<br>documentation standards<br>for tokenised solutions.|Maintain flexibility in<br>internal processes to<br>adapt to evolving<br>regulations, ensuring<br>ongoing compliance and<br>minimising operational<br>disruptions.|
|**Disclosures**|Platform<br>Transparency|Limited visibility into<br>shared ledger platform<br>governance, technical<br>operations, or|Enhance transparency<br>regarding platform<br>governance and key<br>intermediary roles to<br>improve trust and|



01/07/2025 

25 

--- end of page.page_number=25 ---

|**Area**|**Sub-Area**|**Risk Considerations**|**Potential Solutions**|
|---|---|---|---|
|||intermediary roles may<br>impede risk assessment.|facilitate accurate risk<br>assessments.|
||Sustainability|Certain shared ledgers<br>consensus mechanisms<br>have a significant carbon<br>footprint, potentially<br>conflicting with<br>sustainability goals.|Consider adopting more<br>energy-efficient<br>consensus mechanisms<br>and promoting<br>sustainability in shared<br>ledger solutions.|



_Table 3: Risk considerations in implementing shared ledger-based payments and settlement_ 

## **5.2 Risk Mitigants** 

Drawing from the Guardian Fixed Income Framework paper published in 2024[24] , this paper also elaborates on and adapts key risk mitigants for broader application in tokenised markets. |**#**|**Risk Mitigant**|**Description**|
|---|---|---|
|1|Ensure Robust<br>Internal Compliance<br>Functions|Develop and scale internal risk, compliance, and control<br>frameworks to align with the growing complexity of<br>tokenised systems, ensuring continuous monitoring and<br>adaptability.|
|2|Ensure an Effective<br>Incident Response<br>Mechanism|Establish clear, proportionate protocols to address faults in<br>smart contracts or shared ledger discrepancies, minimising<br>potential disruptions.|
|3|Ensure Standardised<br>Smart Contract<br>Audits|Implement recognised audit frameworks and conduct<br>independent security reviews before deployment to<br>proactively identify and rectify vulnerabilities.|
|4|Ensure Clear &<br>Documented Code|Write smart contracts in well-commented, transparent<br>code that facilitates routine audits, maintenance, and<br>updates.|
|5|Ensure Extensive<br>Pre-deployment<br>Testing|Conduct comprehensive simulations and stress tests under<br>diverse scenarios to validate smart contract behaviour and<br>operational resilience before live deployment.|
|6|Ensure a Combined<br>Access & Manual<br>Review Framework|Utilise a unified approach that integrates automated access<br>controls with human intervention to monitor and verify key<br>system functions, reducing the risk of error or malicious<br>activity.|
|7|Ensure Contractual<br>Clarity Among<br>Stakeholders|Clearly define legal roles, responsibilities, and liabilities<br>among all parties through robust contractual agreements<br>to guarantee enforceability and mitigate disputes.|



> 24     https://www.mas.gov.sg/publications/monographs-or-information-paper/2024/guardian-fixed-income-framework 

01/07/2025 

26 

--- end of page.page_number=26 ---

||8|Ensure a Robust<br>Legal Framework for<br>Derivatives|When a technology solution is applied to derivatives<br>trading, there are risks that the resulting contract may lack<br>legal efficacy. To address this, ISDA has published a series of<br>guidelines for smart derivatives contracts—including one<br>on FX derivatives—that explain the core principles of the<br>ISDA documentation and raise awareness of the key legal<br>terms that must be maintained. These guidelines also<br>highlight important issues for technology developers when<br>designing solutions for trading, processing FX, or<br>automating settlement, and point to areas where further<br>industry collaboration is needed to resolve legal and<br>regulatory uncertainty.|
|---|---|---|---|



_Table 4: Key risk mitigants for broader application in tokenised markets_ 

01/07/2025 

27 

--- end of page.page_number=27 ---

## **6 Case studies and examples** 

In this section, we explore case studies of implemented solutions that demonstrate the use of tokenised bank liabilities, more specifically tokenised deposits in transaction banking. A tokenised deposit[25] is a digital representation of a commercial bank deposit issued and managed on a shared ledger. The main reason for leveraging money on deposit is its affinity with the traditional two-tier monetary system and with existing laws and regulations, meaning that innovation can take place within the existing framework of the financial system in a more straightforward way. The terminology of tokenised deposit is used in this paper in a broad, functional sense to refer to tokenised representation of commercial bank deposit on a shared ledger, without asserting any specific legal or regulatory interpretation. ## **6.1 Use case 1:  Ant International - multi-currency tokenised deposit for crosscurrency FX payments[26]** 

## _**Overview**_ 

Ant International, a leading global provider of digital payment and financial technology solutions, supports merchants worldwide of all sizes in achieving their growth goals. Through a comprehensive range of technology-driven services, Ant International collaborates closely with partners to initiate and receive payments across multiple locations 24 x 7.


Final chunk number 25, content : 
Partnering with over 70 global financial institutions, Ant International provides online payment channels serving 1.2 billion buyers and 2 million sellers in more than 200 countries, supporting major global merchants and all Alibaba affiliates.


Final chunk number 26, content : 
By building a distributed financial network, Ant International seeks to transfer fiat currency and on-chain tokenised assets globally with virtually no delay, significantly enhancing liquidity and operational efficiency. This should reduce the costs associated with traditional cross-border payments—transforming a process that could take between one to three days and costing up to $27 on average to complete (excluding FX cost)[27] into one that is instant and cost-effective—but also dramatically shortens the time for funds to clear to minutes or even seconds. Cross-border payments where an exchange of currencies is required constitute a significant percentage in daily corporate treasury management, creating a demand for a technological solution through tokenised payments. Currently, Ant International partners with several Project Guardian participating banks on tokenised deposits issuance with the goal to perform 24 x 7 cross-border transactions for internal liquidity management. However, this is faced by practical challenges including 

> 25   Tokenised deposits are liabilities of the bank that meet the definition and legal characteristics of a deposit according to the relevant legal framework of the jurisdiction in which the deposit is accepted. > 26  The focus of the pilot is on testing the technology and the token exchange model. It does not assert any specific legal or regulatory interpretation on whether the token is a tokenised deposit. > 27  https://www.oliverwyman.com/content/dam/oliver-wyman/v2/publications/2021/nov/unlocking-120-billion-value-in-crossborder-payments.pdf 

01/07/2025 

28 

--- end of page.page_number=28 ---

- FX markets being available only 5 and a half days a week 

- Lack of standard definitions for participants’ role in tokenised deposits 

## _**Business process and solution; the role of a liquidity provider**_ 

A cross-border payment may involve the exchange of tokenised deposits in different currencies, potentially issued by different issuers. To address this cross-currency element, Ant International is piloting an approach for a token exchange model with a “Liquidity Provider” (further described below) to facilitate cross-border payment, while leveraging banking partners to provide off-chain FX pricing through a price oracle. For the pilot use case, Ant International acts as the liquidity provider, but this role can be potentially filled by another market participant, with the relevant licensing requirements, as the solution matures. A liquidity provider’s role is to perform a token exchange between tokens in two currencies and provide a fixed quote price to the end user. In this use case, a banking partner will be providing an off-chain FX price to the price oracle. With this, tokens denominated in different currencies and by different issuers will then be used to complete the cross-border payment. To become a liquidity provider: 

- The liquidity provider must be a legal holder of both tokens in the currency pair and possess a certain amount of each token. - The liquidity provider must have the capability to offer token currency pair pricing, securely, reliably, and in real-time, by using a price oracle to upload the off-chain token pair exchange rates onto the shared ledger. An FX quote oracle provides secure transmission of on-chain and off-chain prices, while the on-chain Multi-Token Swap (MTS) contract facilitates the exchange of different assets and supports automatic AML screening through a user whitelist maintained by the MTS service provider to meet regulatory compliance requirements. 01/07/2025 

29 

--- end of page.page_number=29 ---

**==> picture [398 x 301] intentionally omitted <==**

_Figure 2: Illustration of on-chain MTS smart contract interaction flow_ 

The payment effectively takes place in 3 main stages – issuing, transferring and redeeming; with the token exchange occurring at the transfer token stage. **==> picture [404 x 169] intentionally omitted <==**

_Figure 3: Illustration of payment flow._ 

|**Stage**|**Activity**|
|---|---|
|Issue Token|1. Issuer Bank A in Singapore off-chain debits Ant<br>International’s Entity A cash account for SGD fiat<br>currency<br>2. Issuer mints Bank A SGD token to Ant International’s<br>Entity A’s token address|



01/07/2025 

30 

--- end of page.page_number=30 ---

|Transfer Token|3. SGD token is transferred to the liquidity provider’s<br>token address<br>4. The liquidity provider provides price quote, checks for<br>currency pair availability, relevant screening and<br>performs token exchange (SGD token for USD token).<br>The liquidity provider then transfers USD token to Ant<br>International Entity B’s token address|
|---|---|
|Redeem Token|5. Issuer Bank B burns USD token from Ant International<br>Entity B’s token address<br>6. Issuer Bank B credits Ant International Entity B’s cash<br>account with USD fiat currency.|



_Table 5: Description of payment flow._ 

With this setup, Ant International is able to perform a cross-border cross-currency payment within internal entities. ## _**Key learning points and potential future development**_ 

A token exchange model using a liquidity provider is a potential solution for cross-border payments using tokenised deposits. Leveraging smart contracts, the liquidity provider can perform on-chain fulfilment of the token swap, ensuring transparent, immutable and secure transactions occur in real-time.


Final chunk number 27, content : 
In addition, programmability embedded in the tokens, such as conditional payments, would be able to enhance transaction efficiency and flexibility.


Final chunk number 28, content : 
For example, conditional payments can automate processes such as releasing funds only when predefined conditions are met, reducing the need for intermediaries, lowering cost, and mitigating risks of disputes. This programmability can also enable features like automated compliance checks, escrow arrangements, or milestone-based disbursements, all of which can streamline operations. While liquidity providers are rewarded with liquidity cost and price spread, the entry of more liquidity providers will unlock additional liquidity to the market. Additionally, liquidity providers could exchange tokenised deposits with each other, creating a more robust and interconnected liquidity network. This would further enhance market efficiency by enabling seamless transfers and price discovery across different currencies and platforms.


Final chunk number 29, content : 
For future development, the scope can be expanded to study the feasibility of interoperability with existing FX trading systems. Additionally, as the size of the tokenised deposit market grows in the future, the implication of expanding the liquidity provider model to more participants can be studied, together with its licensing and technology setup requirements. ## **6.2 Use case 2: BNY and OCBC - FX payments through shared ledger interoperability** 

## _**Overview**_ 

BNY and OCBC use case focused on shared ledger interoperability, as the proliferation of new networks with distinct value propositions has led to a fragmented landscape for new clearing and settlement locations, with no clear winner yet. 01/07/2025 

31 

--- end of page.page_number=31 ---

To overcome this and still drive shared ledger adoption, BNY Treasury Services and OCBC collaborated to demonstrate how “Hash-Time Locked Contracts” (further described below) could create interoperability between two private permissioned, bank owned shared ledgers. Each bank operates their own shared ledger as they would today but leverages the technical benefits of modern infrastructure to increase the speed of transaction and the security with which the transaction is processed. The pilot proved the technical feasibility of this solution, including near-instantaneous settlement of the transactions (vs.


Final chunk number 30, content : 
typical cross-border FX payment transactions, which can take up to 2 days to settle). The model provides the immediate benefit of faster settlement speeds for clients, while providing a roadmap for financial institutions to adopt the new technology. In addition to the speed and efficiency improvements demonstrated by the pilot, BNY and OCBC plan to explore design concepts for shared ledger-based fraud mitigation tools that can increase both banks’ ability to identify and flag suspicious activity in near real time. ## _**Business process and solution description**_ 

The originating bank initiates a transaction as they would today, then passes a message via a bilateral connection to the beneficiary’s bank (or correspondent). The payment message will include a secret, which in turn unlocks the smart contract holding the funds in escrow. Once the funds are unlocked from escrow, they are credited to the beneficiary’s account in tokenised deposit form, and then made available for “last mile” pay-out through traditional instant payment rails. The solution relies on private, permissioned shared ledger at both participating financial institutions, as well as a smart contract standard using Hash Time-Locked Contracts to exchange either messages or tokens across the two chains. There is no need for each bank to run on the same type of underlying shared ledger technology (e.g., Hyperledger Besu vs. R3 Corda) in this case, as the tokens are not swapped; only messages exchanged (in the pilot phase). **==> picture [404 x 76] intentionally omitted <==**

_Figure 4: Illustration of the business process and solution description_ 

## _**Key Learnings and future developments/possibilities**_ 

The bilateral connection between the two banks can be replicated across strategic partners, globally. This solution offers an alternative to both existing messaging networks, and the need for a central, single coordination point, enhancing the resiliency of both banks’ operations. There are critical opportunities to introduce new fraud mitigation tools into the process given the “speed bump” introduced by the escrow of funds in the HTLC. Making 

01/07/2025 

32 

--- end of page.page_number=32 ---

payments faster and safer is critical to BNY and OCBC’s strategy and the industry’s continued growth. ## **6.3 Use case 3: HSBC - Payment vs Payment orchestration** 

## _**Overview**_ 

Cross-border FX settlement for inter-bank FX trades can be complex and manual due to different risk management and payment systems. There are industry needs for safer and instantaneous settlement with reduction of settlement and credit risks, improved liquidity pools, and capital efficiencies. A multi-participant PvP solution using shared ledger will be able to mitigate the challenges.


Final chunk number 31, content : 
This solution also supports alignment with FX Global Code Settlement Risk Principles 35 and 50, the Financial Stability Board’s G20 roadmap for enhancing cross-border payments, and the CPMI’s Stage 2 report to the G20. OSTTRA – an industry leading post trade infrastructure solution, provides service offering for participants to match, confirm and pay FX cash flows (PvP) with reduction of Herstatt risk and flexible settlement windows. Outstanding exposures can continuously be netted to reduce daily settlement limits and unlock capital, allowing more trade volumes to be conducted. HSBC’s key role will be as a full supporter and a network participant of this solution across all supported currencies including emerging market currencies (e.g., CNH). HSBC has been using this solution internally for over 6 years, settled over 9.4 Tn USD, and have realised the benefits of using PvP amongst 18 entities. **==> picture [406 x 148] intentionally omitted <==**

_Figure 5: High level illustration of solution design_ 

There are several reasons why the solution has been successful and operational for over 6 years: firstly, the solution relies on fiat money using existing bank account structures; secondly, the solution is an overlay to existing systems and solutions used by the industry; and thirdly, the implementation of shared ledger as a smart workflow and payment orchestration layer leverages existing risk and control frameworks. Current industry challenges also include interoperability across multiple platforms and networks, increasing ecosystems’ complexity and hence operational risks. Migration from legacy to digital can also be costly and takes time. Trading counterparties may not have full 

01/07/2025 

33 

--- end of page.page_number=33 ---

visibility of their forward-looking FX cash flows which may result in missed payments and overdraft fees. Benefits of the OSTTRA PvP solution (by joining as a network participant) will help to solve the following: 

- Lower costs by removing confirmations, reconciliations and external fees. - Single view of settlement lifecycle across multiple global systems. - Transparency of forward-looking FX cash flows.


Final chunk number 32, content : 
- Full audit trail from trade capture to cross-border settlement. - Reduced Herstatt risk through utilisation of shared ledger to orchestrate PvP settlement. - Lower implementation costs, as it’s an overlay of existing infrastructure. - Direct API integration available to allow transfer of funds within secs. Future benefits will be enablement of cash flow compressions of outstanding exposures using ‘settle to market’ payments to reduce counterparty risks and capital requirements. ## _**Business process and solution - straight through processing solution**_ 

The PvP process is designed to follow straight through processing (STP) after onboarding and connectivity has been set up with the OSTTRA shared ledger network. Operational staff will have full graphical user interface (GUI) dashboard for monitoring of the trade lifecycles from trade matching to funding, PvP, and de-funding before completion of the day end obligations. Exceptional management processes are available if counterparties wish to settle outside of the network bi-laterally.


Final chunk number 33, content : 
The detailed steps are as follows: 

- Trading counterparties send their trades to OSTTRA in real time. - Trades will be matched by OSTTRA before inclusion in the netting set by currency and counterparty. - On settlement day, pre-settlement netting amounts are matched and agreed by participants which triggers messages to participants’ post trade systems for funding of their obligations. - Once funds are received from both sides, a PvP event will occur to reflect change of fund ownership (funds are transferred between trading counterparties’ accounts only, OSTTRA is not the intermediary, only acting as an orchestrator of the process). 01/07/2025 

34 

--- end of page.page_number=34 ---

- Funds will move back to designated nostros before market cut-off times. **==> picture [367 x 235] intentionally omitted <==**

_Figure 6: Illustration of business and solution flow_ 

- Participants trading system will need to send trades to OSTTRA in real time. - Participants’ post-trade systems will need to be connected (e.g., MQ) to OSTTRA to receive instructions for trade confirmation and funding events. OSTTRA service relies on existing network for trade matching and shared ledger (provided by Baton Systems) for record of obligations and orchestration. ## _**Key Learnings and future developments/possibilities**_ 

This proposed solution needs a network of participants to realise the benefits. With more emphasis on conducting safer and instantaneous cross-border payments, a shared ledger PvP solution will have a significant role to play for reduction of bi-lateral settlement and counterparty risks. It can also provide better liquidity and a reduction in capital requirements, unlocking more trading and efficiencies. Direct API connectivity with participants also helps to reduce the current network messaging (SWIFT) dependencies & delays from minutes to seconds. OSTTRA (network operator) is already connected with many wholesale banks and corporates from their existing solutions. With their open APIs, onboarding to this network as a participant will cost less to implement, it’s an overlay to existing infrastructures. As the market is trying to solve interoperability, this proposed pilot and recommendation to join can help to solve many of the current challenges. While the suggested use case is mainly for FX related cash flows, this solution is asset class agnostic and so it can easily be expanded for cross asset cash flows in the future. 01/07/2025 

35 

--- end of page.page_number=35 ---

## **7 Developing standardised documentation for tokenised FX transactions** 

The use cases above provide illustrations on possible implementation and potential benefits through the adoption of tokenised bank liabilities and shared ledger solutions. An industry-wide framework that is broadly accepted will accelerate technology adoption by reducing legal uncertainty and standardising operations. This standardisation will help achieve consistency, transparency and efficiency across markets.


Final chunk number 34, content : 
## **7.1** 

## **Existing industry standards** 

Associated standards and frameworks have been developed in the FX market to ensure transparency, efficiency and risk management. These standards are also applicable to transaction banking involving FX payments. They play a crucial role in fostering user confidence and the smooth functioning of the transaction banking market and will act as the foundation for further development alongside the increased use of tokenised bank liabilities. |**Standards**|**Examples**|
|---|---|
|Messaging and<br>communication|•<br>SWIFT Messaging (notably for post-trade and asset<br>servicing).<br>•<br>FIX Protocol (notably for trading)<br>•<br>Adoption of ISO 20022 XML format for trade reporting|
|Standard identifiers<br>for trades and<br>parties|•<br>Examples of instrument identifiers include Unique Trade<br>Identifiers (UTI) and Unique Product Identifiers (UPI)<br>•<br>Legal Entity Identifier (LEI).|
|Settlements|•<br>Settlement cycles – T+2 for the vast majority of currencies<br>and T+1 for USD/CAD, USD/TRY, USD/PHP and USD/RUB.<br>•<br>Ideally, PVP settlement and ideally net settlement to<br>alleviate daylight (or Herstatt) risk.28|
|Regulatory<br>compliance and<br>reporting obligations|•<br>Regulatory compliance and reporting are essential for<br>market integrity, customer protection, and financial<br>stability. Examples include:<br>-<br>Trade reporting for example the MFID II pre and post<br>trade reporting in the EU and Dodd-Frank Act in the<br>US OTC derivatives products, for which ISDA has<br>developed the Digital Regulatory Reporting solution<br>using the common domain model|



> 28 The most effective netting solution is multilateral as provided by CLSSettlement which settles over USD 6.5TN of payments daily on a PvP basis in 18 of the most actively traded currencies globally. This approach mitigates settlement risk and reduces funding costs by 96%. The alternative to multilateral netting is bilateral. In its December 2019 Quarterly Review, the BIS identified that of the USD 18.7TN daily gross payment obligations arising from all FX instruments, USD 6.3TN were settle on a PvP basis and USD 3.5TN were settled using bilateral netting. https://www.bis.org/publ/qtrpdf/r_qt1912.pdf 

01/07/2025 

36 

--- end of page.page_number=36 ---

|**Standards**|**Examples**|
|---|---|
||-<br>Business conduct disclosure standards, to which end<br>ISDA has published the Foreign Exchange Disclosure<br>Annex to the DFA Disclosure and other standards.|
|Industry best<br>practices|•<br>FX Global Code29|
|Documentation|•<br>ISDA Master Agreement<br>•<br>The 1998 FX and Currency Option Definitions jointly<br>published by ISDA, EMTA and the Foreign Exchange<br>Committee.|
|Messaging and<br>communication|•<br>SWIFT Messaging (notably for post-trade and asset<br>servicing).<br>•<br>FIX Protocol (notably for trading)<br>•<br>Adoption of ISO 20022 XML format for trade reporting|



_Table 6: Current Standards and Frameworks Governing the FX Market_ 

## **7.2 Ongoing industry initiatives and regulatory developments** 

Internationally, policymakers are leading efforts in exploring the use of tokenised bank liabilities and shared ledgers in settlements. Apart from the BIS and FSB examples cited, other examples include: 

- **Germany** : The German Banking Industry Committee (GBIC) has published a whitepaper on the Commercial Bank Money Token (CBMT). - **Hong Kong** : The Hong Kong Government has launched Project Ensemble, a wholesale central bank digital currency (wCBDC) project to support the development of the tokenisation market in Hong Kong.


Final chunk number 35, content : 
One of the sandbox pilot strands is to encourage the use of tokenised deposits. - **Singapore** : The Monetary Authority of Singapore (MAS) has announced the development of an SGD Testnet to facilitate financial institutions access to common settlement assets for market testing purpose. - **South Korea** : In South Korea, a live pilot of tokenised deposits, involving 100,000 individuals, started during the October-December quarter of 2024. - **United Kingdom** : UK Finance, an industry group representing the financial services industry, has worked with a number of its members and partners on a new Regulated Liability Network (RLN) experimentation phase.[30] 

> 29 The FX Global Code is a set of global principles of good practice in the foreign exchange market, developed to provide a common set of guidelines to promote the integrity and effective functioning of the wholesale foreign exchange market. https://www.globalfxc.org/fx-global-code/. > 30 Further information available at: https://www.ukfinance.org.uk/news-and-insight/press-release/uk-finance-announces-successfuloutcome-regulated-liability-network. 01/07/2025 

37 

--- end of page.page_number=37 ---

- **Project Agorá** , led by the BIS Innovation Hub, together with seven central banks and commercial banks from each jurisdiction, will test for improvements in the speed and cost of cross-border payments by utilising technologies such as tokenisation and “smart contracts”. ## _**Development of consistent industry standards for data**_ 

To this end, consistent digital representation standards will also facilitate the development of smart contracts. The Common Domain Model[31] (“CDM”) is a standardised, machinereadable, and machine-executable data and process model for how financial products are traded and managed across the transaction lifecycle.


Final chunk number 36, content : 
Adoption of the CDM will enable a consistent hierarchical representation of trade data across trades, portfolios and events. It also allows for standard processing of trade lifecycle events, such as reporting under ISDA’s Digital Regulatory Reporting initiative, which significantly reduces the time, resources and cost needed to implement reporting regulations in multiple jurisdictions.[32] While the CDM was initially launched for derivatives, it is now used for repos, securities lending and bonds and is hosted by the Fintech Open Source Foundation (FINOS). ## _**Value of standardised documentation**_ 

Standardised documents play a critical role in financial transactions by promoting consistency, transparency, and efficiency across markets. They reduce legal uncertainty by providing commonly accepted terms and conditions, which help parties understand their rights and obligations clearly.


Final chunk number 37, content : 
This consistency minimises the risk of disputes and litigation, fostering smoother negotiations and faster execution of transactions. Ultimately, it enhances market stability, reduces transaction costs, and promotes broader participation in global financial markets, especially in complex transactions like derivatives. ## _**Developing standardised documentation for tokenised deposits**_ 

The Project Guardian FX workstream has identified that one of the practical challenges is the lack of standard terms in FX transactions using tokenised bank liabilities. Specifically for tokenised deposits, ISDA has been asked to develop industry standard documentation to facilitate use of tokenised deposits in FX transactions. One approach would be to leverage the existing ISDA documentation framework and develop model provisions (the “ **Additional Provisions** ”) for parties that wish to settle deliverable FX spot, forward, and swap transactions under an ISDA Master Agreement using deposit tokens, each denominated in a single fiat currency and issued or held on a shared ledger-based settlement platform. The Additional Provisions contemplate that such transactions will incorporate the definitions and provisions contained in the 1998 FX and Currency Option Definitions as published by ISDA, Emerging Markets Traders Association and the Foreign Exchange Committee (the “ **1998 FX Definitions** ”)[33] , or, as applicable, the 2021 ISDA Interest Rate Derivatives 

> 31  Further information about the common domain model is available at https://www.finos.org/common-domain-model. > 32  Further information about the ISDA Digital Regulatory Reporting initiative is available at https://www.isda.org/isda-solutionsinfohub/isda-digital-regulatory-reporting/. > 33  ISDA is in the process of updating the 1998 FX Definitions, with the industry implementation phrase set to run from late 2025 to November 2027. 01/07/2025 

38 

--- end of page.page_number=38 ---

Definitions (the “ **2021 Definitions** ”) as published by ISDA. The chart below illustrates the ISDA documentation framework for FX transactions: 

**==> picture [403 x 153] intentionally omitted <==**

_Figure 7: FX and Currency Options Documentation Architecture_ 

## _**Changes to the FX Definitions to accommodate tokenised models**_ 

The Additional Provisions assume that the tokens in question record a sum of fiat cash in an account with the relevant participating banks, and are in registered or claims form. The liquidity provider will hold a pool of tokens from the participating banks which it will use to provide liquidity services to users. A user who holds tokenised deposits in one currency and wants to exchange it for another currency may enter into an FX transaction with the liquidity provider. The amendments assume that the liquidity provider and each user have signed, a 2002 ISDA Master Agreement and envisage that the transactions will be documented using a deliverable FX confirmation incorporating the 1998 FX and Currency Option Definitions 1998 FX Definitions as supplemented by these additional provisions. Although the Additional Provisions are drafted for the specific use case envisaged by the Project Guardian industry pilots, they may further be used as a reference and adapted as necessary, taking into account differences in token design and transaction structures. Adapting the “Additional Provisions” to accommodate tokenised models would require amendments to capture changes to timing, operating models, and the role of platforms. Preliminary studies indicate that these changes could potentially include: 

- changes to the “Business Day” definition to permit 24/7 settlement; 

- including a definition of “tokenised deposits” and clarifying that cash and currency includes tokenised deposits; 

- clarifying how payments under the transactions will be made in the context of tokenised deposits; 

- including, to the extent needed, contingency provisions to cater for platform related events; and 

- including representations from counterparties of their continued access to the platform in order to allow settlement on platform. 01/07/2025 

39 

--- end of page.page_number=39 ---

## _**Enforceability of close-out netting and collateral arrangements**_ 

The enforceability of the ISDA Master Agreement and Credit Support Documents are supported by the netting and collateral opinions obtained by ISDA.[34] The principal focus of the opinions has always been on ensuring enforceability of netting and a related collateral arrangement against a party that is subject to insolvency proceedings. This is because mandatory insolvency rules come into operation that could potentially disrupt close-out netting and/or a related collateral arrangement. Applying existing insolvency law rules to a new asset class inevitably raises legal characterisation and other questions that must be tackled to provide the necessary certainty. ISDA has published a white paper[35] exploring the application of close-out netting to digital asset derivatives and the enforceability of collateral arrangements that involve transfers or exchanges of digital assets. The insolvency laws in each applicable jurisdiction should be considered in the context of tokenised assets. In the jurisdictions which have enacted specific legislation providing the legal basis for the issuance and ownership status of shared ledger-based tokens, the treatment of such tokens in an insolvency situation may be expressly catered for. In jurisdictions without specific enabling legislation, general insolvency laws principles will need to be applied. In the context of tokenised deposits, the nature of the customer’s rights against an issuer will lend to the nature of its claim against that entity in any insolvency proceedings. > 34 A list of the jurisdictions from which ISDA has obtained netting and collateral opinions appears on the ISDA website at www.isda.org, together with a list of the jurisdictions around the world that have enacted or are considering enacting netting legislation. 35 https://www.isda.org/2023/01/26/navigating-bankruptcy-in-digital-asset-markets-netting-and-collateral-enforceability/ 

01/07/2025 

40 

--- end of page.page_number=40 ---

## **8 Conclusion** 

The future of finance points towards an interconnected ecosystem where tokenised assets can be traded and settled globally, with settlement finality between counterparties. Achieving this vision requires progress on two fronts. First, the development of multipurpose shared ledger infrastructures that can support the exchange of tokenised assets and money while meeting regulatory expectations. Secondly, the development of robust connectivity protocols with liquidity providers serving as intermediaries to bridge different ledger platforms **[36] ,** and across multiple trading venues.[37] 

## _**Moving forward**_ 

Financial institutions would need to adapt their existing processes and infrastructures to prepare for the growing tokenised asset market. This paper has examined several use cases that demonstrate how FX transactions can integrate with tokenised asset infrastructures, highlighting how shared ledger networks can enhance operational efficiency. As both the technology and protocols mature, and as industry participants advance their applications, current interoperability challenges with existing systems will likely be resolved. Innovation and transformation in financial markets must extend beyond individual asset classes and institutions. Project Guardian provides a platform for industry participants to develop common standards across different capital markets products, enabling tokenised assets to scale sustainably and pool liquidity. The improved efficiencies achieved in transaction banking could generate broader benefits across financial market and enhance settlement processing for other asset classes[38] : 

- **Liquidity Optimisation:** Enhanced transaction banking efficiency could free up liquidity by reducing funds in transit and capital tied up in for pre-funding of payments. However, it is imperative to address the potential fragmentation of the liquidity pool and fungibility of liquidity pools arising from programmability of tokenised bank liabilities. - **Risk Reduction:** Streamlined cross-border payments and FX settlements could minimise counterparty and settlement risk, given that a common risk-free settlement asset and settlement finality could be jointly adopted by the market. - **Market Standardisation:** The adoption of tokenised bank liabilities by more market participants may encourage broader standardisation across the different systems, 

> 36 As discussed in the IMF Article _A Digital Marketplace to Improve Cross-Border Payments_ ; at https://www.imf.org/en/Publications/fintech-notes/Issues/2023/03/03/Trust-Bridges-and-Money-Flows-A-Digital-Marketplaceto-Improve-Cross-Border-Payments-528038. > 37 Another example is the Regulated Liability Network (RLN) proposal for a regulated financial market infrastructure that can deliver an interoperable network for various facets of the sovereign currency system: central bank money, commercial bank money, e- money and regulated stablecoins. https://regulatedliabilitynetwork.org/. The IMF (see paper in footnote 2) has considered a global clearinghouse to intermediate swap arrangements between central banks. > 38   The BIS Reports on Tokenised Assets has explored the implications of tokenisation on financial markets, including its potential to reduce settlement risks and enhance market efficiency. Studies on Continuous Linked Settlement (CLS) and Payment-versusPayment (PVP) systems have demonstrated how reducing settlement risk in one area of FX can positively influence broader markets. Various papers from SWIFT, ISDA, and financial institutions often discuss the cascading effects of technological adoption in financial services. 01/07/2025 

41 

--- end of page.page_number=41 ---

creating a foundation for interoperability and consistency in processing settlements. This could have positive implications for how settlements are handled across multiple asset classes, including equities, commodities, and bonds. - **Broader Adoption of Digital Solutions:** Successful implementation in transaction banking use cases could act as a catalyst for wider industry adoption of tokenisation and shared ledger-based solutions, which could then drive improved efficiencies across different use cases benefiting from atomic payments and smart contracts.


Final chunk number 38, content : 
- **Increased Market Confidence:** Innovations that prove successful in digital payments including tokenised transfers may inspire greater trust and confidence in the scalability and reliability of such technologies. This, in turn, could attract participants in other asset classes to adopt these solutions. In conclusion, tokenised bank liabilities and payments have the potential to enhance efficiency, reduce risk, and improve liquidity in FX markets. Realising these benefits will require continued collaboration between industry participants and regulators to address key challenges, including the alignment of legal frameworks, the development of robust operational standards, and the refinement of regulatory treatment and compliance processes. While there are transitional challenges as traditional and emerging systems co-exist, these are not insurmountable.


Final chunk number 39, content : 
With thoughtful engagement and shared commitment, the industry is well-positioned to develop the clarity and best practices needed to support the safe and effective adoption of tokenised bank liabilities. As the ecosystem matures, these collective efforts will help lay the foundation for a more efficient, resilient, and inclusive global financial system. 01/07/2025 

42 

--- end of page.page_number=42 ---

## **9 References** 

|1.|Financial Stability Board (FSB) (2024).G20 Roadmap for Enhancing Cross-border|
|---|---|
||Payments.|
|2.|Bank for International Settlements (BIS) (2022).Extending and aligning payment system|
||operating hours for cross-border payments.|
|5.|BIS (2023).Blueprint for the future monetary system: improving the old, enabling the|
||new.|
|6.|Ibid.|
|7.|FSB (2020).Enhancing Cross-Border Payments: Stage 3 Roadmap.|
|8.|CLSSettlement.CLS Settlement.|
|10.|Bank of England (BOE) (2024).Once more unto the breach.|
|11.|FSB (2024).FSB Annual Progress Report on Meeting the Targets for Cross-border|
||Payments, 2024 Report on Key Performance Indicators.|
|12.|BIS (2020). Payments without borders.|
|13.|BIS (2002).BIS Quarterly Review.|
|14.|Monetary Authority of Singapore (MAS) (2023). Project Guardian Open and|
||Interoperable Networks.|
|15.|Ibid.|
|17.|Monetary Authority of Singapore (MAS) (2023).Project Guardian Interlinking Networks.|
|19.|Monetary Authority of Singapore (MAS) (2024).Project Guardian Fixed Income|
||Framework.|
|22.|Global Layer 1 (GL1) (2024).Global Layer 1 Whitepaper|
|23.|GL1 (2024). Programmable Compliance Toolkit.|
|24.|Monetary Authority of Singapore (MAS) (2024).Project Guardian Fixed Income|
||Framework.|
|27.|Oliver Wyman, J.P. Morgan (2021).Unlocking $120 Billion Value In Cross-Border|
||Payments.|
|28.|BIS (2019).BIS Quarterly Review.|
|29.|Global Foreign Exchange Committee (2024).FX Global Code.|
|30.|UK Finance (2024).UK Finance announces successful outcome of Regulated Liability|
||Network Experimentation Phase.|
|31.|Fintech Open Source Foundation (FINOS).Common Domain Model.|
|32.|International Swaps and Derivatives Association (ISDA).ISDA Digital Regulatory|
||Reporting.|
|34.|ISDA (2025).Status of Netting Legislation.|



01/07/2025 

43 

--- end of page.page_number=43 ---

35. ISDA (2023). Navigating Bankruptcy in Digital Asset Markets Netting and Collateral Enforceability.


Final chunk number 40, content : 
36. International Monetary Fund (IMF) (2023).


Final chunk number 41, content : 
Trust Bridges and Money Flows: A Digital - 

Marketplace to Improve Cross Border Payments. 37. Regulated Liability Network (RLN).


Final chunk number 42, content : 
(2022). The Regulated Liability Network Digital Sovereign Currency.


Final chunk number 43, content : 
38. BIS (2024). Report for the G20 on tokenisation highlights the opportunities, risks and future considerations for central banks. 01/07/2025 

44 

--- end of page.page_number=44 ---

01/07/2025 

45 

--- end of page.page_number=45 ---



