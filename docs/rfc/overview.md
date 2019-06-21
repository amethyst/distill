I've been working on an implementation of the ideas in the Asset Pipeline RFC lately and I now feel ready to present some technical details and concrete proposals.

# Table of Contents

- [Tracking Issues](#tracking-issue)
  - [Amethyst Community Forum Discussion](#forum-discussion)
- [Motivation]
- [Guide Level Explanation](#guide-level-explanation)
- [Reference Level Explanation](#reference-level-explanation)
- [Drawbacks]
- [Rationale and Alternatives](#rationale-and-alternatives)
- [Prior Art](#prior-art)
- [Unresolved Questions](#unresolved-questions)

# Basic Info
[basic]: #basic-info

- Feature Name: asset_pipeline
- Start Date: (fill me in with today's date, YYYY-MM-DD)
- RFC PR: (leave this empty until a PR is opened)
- [Tracking Issue](#tracking-issue): (leave this empty)
- [Forum Thread](#forum-discussion): (if there is one)

# Summary
[summary]: #summary

With the asset pipeline we introduce an "offline" workflow for importing source files and building assets to be loaded in the Amethyst engine. It aims to enable a number of features.
- Hot reloading of any asset
- Deterministic builds and build artifact caching
- Asset dependency graphs
- Offline optimization and preprocessing of assets with settings specific to each target platform
- Scalable importing and building that can take advantage of available computing resources
- Searching and querying based on tags extracted by asset importers
- Moving and renaming assets without breaking references

The required changes to Amethyst as an engine are comprehensive but finite. To make things easier I will try to split the RFC into a number of smaller RFCs that describe specific changes. These changes may have dependencies on each other and some may not make sense to implement independently, but are broken up to make digesting them easier.

RFCs
- AssetID - unified system of referencing any individual asset that is loaded from a source file
- Format rework - new traits to replace assets::Format: Importer and Builder that generate metadata and enable deterministic builds and caching of build artifacts
- TypeUUID and type registry - dynamic serialization and deserialization using type_uuid and a type registry
- Reflection - runtime fixup of AssetID -> Handle at deserialize time
- AssetHub - daemon/service that runs on development machines and watches project directories for changes, performs imports and maintains metadata
- RPC - protocol for communication between tooling (AssetHub primarily) and engine
- Asset packing - packing assets into a format that is suitable for distribution
- Asset loading - new AssetLoader implementation that works with AssetIDs and Handles
- Prefab rework - new implementation of Prefabs that is defined in terms of components and asset references instead of embedding assets in a prefab.
- GLTF Importer - new implementation of gltf importer that extracts individually addressable assets (including prefab)
- RON prefab - new implementation of ron prefab importer that uses AssetIDs
- Configs - use AssetIDs and new loader to load configs in addition to content assets


## Amethyst Community Forum Discussion
You can access our forums at https://community.amethyst-engine.org

[forum-discussion]: #forum-discussion
<details>
<summary>Information about pre-RFC discussion on our community forum</summary>
There is a category on our forums for what can be considered pre-RFC discussion. It is a good place to get some quick feedback from the community without having to go through the entire process.

This is not required, but if one exists and contains useful information, you may place a link to it here.
</details>

# Motivation
[motivation]: #motivation
Why are we doing this? What use cases does it support? What is the expected outcome?

# Guide-Level Explanation
[guide-level-explanation]: #guide-level-explanation
<details>

<summary>Non-technical overview and reasoning for it.</summary>
Explain the proposal as if it was already included in the language and you were teaching it to another Amethyst programmer. That generally means:

- Introducing new named concepts.
- Explaining the feature largely in terms of examples.
- Explaining how Amethyst developers should *think* about the feature, and how it should impact the way they use Amethyst. It should explain the impact as concretely as possible.
- If applicable, provide sample error messages, deprecation warnings, or migration guidance.
- If applicable, describe the differences between teaching this to existing Amethyst programmers and new Amethyst programmers.

For implementation-oriented RFCs (e.g. for changes to the engine), this section should focus on how engine contributors should think about the change, and give examples of its concrete impact. For policy RFCs, this section should provide an example-driven introduction to the policy, and explain its impact in concrete terms.
</details>

# Reference-Level Explanation
[reference-level-explanation]: #reference-level-explanation
<details>
<summary>The technical details and design of the RFC.</summary>
This is the technical portion of the RFC. Explain the design in sufficient detail that:

- Its interaction with other features is clear.
- It is reasonably clear how the feature would be implemented.
- Corner cases are dissected by example.

The section should return to the examples given in the previous section, and explain more fully how the detailed proposal makes those examples work.
</details>

# Drawbacks
[drawbacks]: #drawbacks

Why should we *not* do this?

# Rationale and Alternatives
[rationale-and-alternatives]: #rationale-and-alternatives

- Why is this design the best in the space of possible designs?
- What other designs have been considered and what is the rationale for not choosing them?
- What is the impact of not doing this?

# Prior Art
[prior-art]: #prior-art
<details>
<summary>Discuss previous attempts, both good and bad, and how they relate to this proposal.</summary>
A few examples of what this can include are:

- For engine, network, web, and rendering proposals: Does this feature exist in other engines and what experience has their community had?
- For community proposals: Is this done by some other community and what were their experiences with it?
- For other teams: What lessons can we learn from what other communities have done here?
- Papers: Are there any published papers or great posts that discuss this? If you have some relevant papers to refer to, this can serve as a more detailed theoretical background.

This section is intended to encourage you as an author to think about the lessons from other engines, provide readers of your RFC with a fuller picture.
If there is no prior art, that is fine - your ideas are interesting to us whether they are brand new or if it is an adaptation from other engines.
</details>

# Unresolved Questions
[unresolved-questions]: #unresolved-questions
<details>
<summary>Additional questions to consider</summary>

- What parts of the design do you expect to resolve through the RFC process before this gets merged?
- What parts of the design do you expect to resolve through the implementation of this feature before stabilization?
- What related issues do you consider out of scope for this RFC that could be addressed in the future independently of the solution that comes out of this RFC?
</details>

Copyright 2018 Amethyst Developers