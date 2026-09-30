// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { GovernanceParameters } from '@daml.js/splice-amulet/lib/Splice/DecentralizedSynchronizer';
import type { ConfigChange } from '../../utils/types';
import { validateDiscountFactor } from './formValidators';

export type GovernanceParameterKey = keyof GovernanceParameters;

/** The form's string value for each parameter. */
export type GovernanceParameterValues = Record<GovernanceParameterKey, string>;

type GovernanceParameterFields = {
  [K in GovernanceParameterKey]: {
    label: string;
    subtitle: string;
    validate: (value: string) => string | false;
    toForm: (value: GovernanceParameters[K]) => string;
    fromForm: (value: string) => GovernanceParameters[K];
  };
};

// One entry per field of the Daml GovernanceParameters record. A set-parameters vote replaces
// the whole record, so a field added in Daml has to be stated too: this stops compiling until
// it has an entry, and the form renders one input per entry rather than a fixed list.
export const governanceParameterFields: GovernanceParameterFields = {
  discountFactor: {
    label: 'Traffic Discount',
    subtitle:
      "Multiplies this synchronizer's traffic price, greater than 0 and at most 1. 1 is the full price.",
    validate: value => validateDiscountFactor(value.trim()),
    toForm: value => value,
    fromForm: value => value.trim(),
  },
};

export const governanceParameterKeys = Object.keys(
  governanceParameterFields
) as GovernanceParameterKey[];

export const toGovernanceParameterValues = (
  parameters: GovernanceParameters
): GovernanceParameterValues =>
  Object.fromEntries(
    governanceParameterKeys.map(key => [
      key,
      governanceParameterFields[key].toForm(parameters[key]),
    ])
  ) as GovernanceParameterValues;

export const fromGovernanceParameterValues = (
  values: GovernanceParameterValues
): GovernanceParameters =>
  Object.fromEntries(
    governanceParameterKeys.map(key => [key, governanceParameterFields[key].fromForm(values[key])])
  ) as GovernanceParameters;

/** One row per parameter; the current value is left empty when it is not known. */
export const governanceParameterChanges = (
  current: GovernanceParameters | undefined,
  proposed: GovernanceParameters
): ConfigChange[] =>
  governanceParameterKeys.map(key => ({
    fieldName: key,
    label: governanceParameterFields[key].label,
    currentValue: current ? governanceParameterFields[key].toForm(current[key]) : '',
    newValue: governanceParameterFields[key].toForm(proposed[key]),
  }));
