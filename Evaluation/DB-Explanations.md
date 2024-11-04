**Describes the tables in the datbase Evaluation.db**

* MappingSetup: 
  * contains a unique mapping_id for each utilised setup of parameters.
  Basically in MappingSetup all parameters and configurations are shown, that are relevant to running the local expansion strategy.

* DbFingerPrint: 
  * Contains the number of facts & elements for each database instance
* DbConfig: 
  * Contains pairs of database instances, the number of equal facts of each pair, and a specific use
      the column use is called to select all database pairs of a certain typ for the evaluation.
      Important uses:
    * expansion-same-lib (for Local Expansion Strategy incl. the Quantile)
    * structural-evaluation (for Structural Similarity Metrics)
    * reasoning-evaluation-same-lib (for Reasoning -Evaluation with database pairs of the same library)
    * expansion-cross-lib (for Reasoning -Evaluation with database pairs of different libraries)

* LexicalResults: 
  * for the 5 rename-tables variation of the nr_fake_pairs (how difficult was the masked selection of the correct pair)
      and the utilisation of the numerical improvement to detect things like arg40 ~ arg39.

* StructuralResults: 
  * Uses the full expansion strategy (Anchor quantile = 0 on identical database pairs)

* ExpansionResults: 
  * Contains results for the normal expansion
      (including the Mixed Results, where each components of the mixed similarity was weighted
      according to the importance factor gamma)

* ExpansionResults_MIX_No_Gamma: 
  * Contains the Expansion results for the mixed metric,
      where the individual metrics did not use their importance weight

* FinalMappingResults: 
  * contains the results of computing the mapping function for database pairs for Reasoning-Evaluation

* DLResults: 
  * Contains the Nemo-Runtimes for the two program Control Flow Graph (CFG) and Pointer Analysis (PA)
                  and the overlap after applying the analysis
  * if mapping_id='separate' there was no mapping, the two database were evaluated separately by the non-modifier analyses