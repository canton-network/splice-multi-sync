// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type {
  AmuletRules_ActionRequiringConfirmation,
  DsoRules_ActionRequiringConfirmation,
} from '@daml.js/splice-dso-governance/lib/Splice/DsoRules';
import type { SupportedActionTag } from './types';

// Every vote action defined in Daml has a UI decision recorded here. A vote the UI does not
// support fails on its page with "Unsupported Action", so an action that SVs cast by hand
// must not be added in Daml without also being added to the UI.
//
// Both records are exhaustive over the generated tag unions: adding an action in Daml stops
// this file compiling until the action is classified, and removing one does too. The
// assertion at the bottom then requires every UI vote to be in SupportedActionTag and
// every SupportedActionTag to be classified as a UI vote.

type ActionCoverage =
  | { readonly kind: 'ui-vote' }
  | { readonly kind: 'no-ui'; readonly reason: string };

const uiVote = { kind: 'ui-vote' } as const;
const noUi = (reason: string) => ({ kind: 'no-ui', reason }) as const;

const automation = noUi('Raised by SV automation as a confirmation, not cast by hand.');
const deprecated = noUi('Deprecated in Daml in favour of CRARC_SetConfig.');

export const dsoActionCoverage = {
  SRARC_AddSv: noUi(
    'Upstream offers no UI; SVs are normally onboarded through SRARC_ConfirmSvOnboarding.'
  ),
  SRARC_OffboardSv: uiVote,
  SRARC_ConfirmSvOnboarding: automation,
  SRARC_GrantFeaturedAppRight: uiVote,
  SRARC_RevokeFeaturedAppRight: uiVote,
  SRARC_SetConfig: uiVote,
  SRARC_UpdateSvRewardWeight: uiVote,
  SRARC_CreateExternalPartyAmuletRules: automation,
  SRARC_CreateTransferCommandCounter: automation,
  SRARC_CreateUnallocatedUnclaimedActivityRecord: uiVote,
  SRARC_CreateBootstrapExternalPartyConfigStateInstruction: automation,
  SRARC_UpdateFeaturedAppRight: uiVote,
  SRARC_RegisterSynchronizer: uiVote,
  SRARC_ArchiveSynchronizerRegistration: uiVote,
  SRARC_SetSynchronizerGovernanceParameters: uiVote,
} satisfies Record<DsoRules_ActionRequiringConfirmation['tag'], ActionCoverage>;

export const amuletActionCoverage = {
  CRARC_MiningRound_StartIssuing: automation,
  CRARC_MiningRound_Archive: automation,
  // Deprecated too, but still rendered so that an old vote remains readable.
  CRARC_AddFutureAmuletConfigSchedule: uiVote,
  CRARC_RemoveFutureAmuletConfigSchedule: deprecated,
  CRARC_UpdateFutureAmuletConfigSchedule: deprecated,
  CRARC_SetConfig: uiVote,
  CRARC_StartProcessingRewardsV2: automation,
  CRARC_StartProcessingExtensionRewardsV2: noUi(
    'PoC extension reward reporting: a voted action with no UI yet (known gap).'
  ),
} satisfies Record<AmuletRules_ActionRequiringConfirmation['tag'], ActionCoverage>;

type UiVoteTagsOf<T> = {
  [K in keyof T]: T[K] extends { kind: 'ui-vote' } ? K : never;
}[keyof T];

type UiVoteTag = UiVoteTagsOf<typeof dsoActionCoverage> | UiVoteTagsOf<typeof amuletActionCoverage>;

type Exactly<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Assert<T extends true> = T;

/** Fails to compile unless the UI votes above are exactly SupportedActionTag. */
export type UiVoteCoverageCheck = Assert<Exactly<UiVoteTag, SupportedActionTag>>;
