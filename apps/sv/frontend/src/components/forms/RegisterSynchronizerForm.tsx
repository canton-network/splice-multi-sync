// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import dayjs from 'dayjs';
import { useState } from 'react';
import { dateTimeFormatISO } from '@canton-network/splice-common-frontend-utils';
import { ActionRequiringConfirmation } from '@daml.js/splice-dso-governance/lib/Splice/DsoRules';

import { useAppForm } from '../../hooks/form';
import { useDsoInfos } from '../../contexts/SvContext';
import { useProposalMutation } from '../../hooks/useProposalMutation';
import { createProposalActions, getInitialExpiration } from '../../utils/governance';
import { CommonProposalFormData } from '../../utils/types';
import {
  CREATE_PROPOSAL_LABEL_EFFECTIVE_AT,
  CREATE_PROPOSAL_LABEL_PROPOSAL_SUMMARY,
  CREATE_PROPOSAL_LABEL_PROPOSAL_TYPE,
  CREATE_PROPOSAL_LABEL_SUPPORTING_URL,
  CREATE_PROPOSAL_LABEL_THRESHOLD_DEADLINE,
  SUPPORTING_URL_PLACEHOLDER,
  THRESHOLD_DEADLINE_SUBTITLE,
} from '../../utils/constants';
import {
  validateDiscountFactor,
  validateOutageAdvance,
  validateEffectiveDate,
  validateExpiration,
  validateExpiryEffectiveDate,
  validatePartyId,
  validateSummary,
  validateSynchronizerId,
  validateUrl,
} from './formValidators';
import { EffectiveDateField } from '../form-components/EffectiveDateField';
import { ProposalSubmissionError } from '../form-components/ProposalSubmissionError';
import { ProposalSummary } from '../governance/ProposalSummary';
import { FormLayout } from './FormLayout';

interface ExtraFormField {
  synchronizerId: string;
  operator: string;
  /** Stated on every registration: 1 is the no-discount value, not an absent one. */
  discountFactor: string;
  /** Bytes per member; 0 is the no-advance value. */
  outageAdvance: string;
}

export type RegisterSynchronizerFormData = CommonProposalFormData & ExtraFormField;

/** The no-discount value. A registration states its parameters rather than defaulting them. */
const DEFAULT_DISCOUNT_FACTOR = '1.0';

/** The no-advance value. */
const DEFAULT_OUTAGE_ADVANCE = '0';

export const RegisterSynchronizerForm: React.FC = _ => {
  const dsoInfosQuery = useDsoInfos();
  const initialExpiration = getInitialExpiration(dsoInfosQuery.data);
  const initialEffectiveDate = dayjs(initialExpiration).add(1, 'day');
  const [showConfirmation, setShowConfirmation] = useState(false);
  const mutation = useProposalMutation();

  const createProposalAction = createProposalActions.find(
    a => a.value === 'SRARC_RegisterSynchronizer'
  );

  const defaultValues: RegisterSynchronizerFormData = {
    action: createProposalAction?.name || '',
    expiryDate: initialExpiration.format(dateTimeFormatISO),
    effectiveDate: {
      type: 'custom',
      effectiveDate: initialEffectiveDate.format(dateTimeFormatISO),
    },
    url: '',
    summary: '',
    synchronizerId: '',
    operator: '',
    discountFactor: DEFAULT_DISCOUNT_FACTOR,
    outageAdvance: DEFAULT_OUTAGE_ADVANCE,
  };

  const form = useAppForm({
    defaultValues,

    onSubmit: async ({ value }) => {
      const discountFactor = value.discountFactor.trim();
      const outageAdvance = value.outageAdvance.trim();
      const action: ActionRequiringConfirmation = {
        tag: 'ARC_DsoRules',
        value: {
          dsoAction: {
            tag: 'SRARC_RegisterSynchronizer',
            value: {
              synchronizerId: value.synchronizerId,
              operator: value.operator,
              // The same vote sets the parameters, so a synchronizer promised a discount is
              // never registered at the full price while a second vote is arranged.
              governanceParameters: { discountFactor, outageAdvance },
            },
          },
        },
      };

      if (!showConfirmation) {
        setShowConfirmation(true);
      } else {
        await mutation.mutateAsync({ formData: value, action }).catch(e => {
          console.error(`Failed to submit proposal`, e);
        });
      }
    },

    validators: {
      onChange: ({ value }) => {
        return validateExpiryEffectiveDate({
          expiration: value.expiryDate,
          effectiveDate: value.effectiveDate.effectiveDate,
        });
      },
    },
  });

  return (
    <FormLayout
      form={form}
      id="register-synchronizer-form"
      actionName={form.state.values.action}
      isReviewStep={showConfirmation}
    >
      {showConfirmation ? (
        <ProposalSummary
          actionName={form.state.values.action}
          url={form.state.values.url}
          summary={form.state.values.summary}
          expiryDate={form.state.values.expiryDate}
          effectiveDate={form.state.values.effectiveDate.effectiveDate}
          formType="register-synchronizer"
          synchronizerId={form.state.values.synchronizerId}
          operator={form.state.values.operator}
          discountFactor={form.state.values.discountFactor.trim()}
          outageAdvance={form.state.values.outageAdvance.trim()}
          onEdit={() => setShowConfirmation(false)}
          onSubmit={() => {}}
        />
      ) : (
        <>
          <form.AppField name="action">
            {field => (
              <field.ProposalTypeField
                id="register-synchronizer-action"
                title={CREATE_PROPOSAL_LABEL_PROPOSAL_TYPE}
              />
            )}
          </form.AppField>

          <form.AppField
            name="synchronizerId"
            validators={{
              onBlur: ({ value }) => validateSynchronizerId(value),
              onChange: ({ value }) => validateSynchronizerId(value),
            }}
          >
            {field => (
              <field.TextField
                title="Synchronizer ID"
                id="register-synchronizer-synchronizer-id"
                subtitle="The id of the dedicated synchronizer to register, as name::fingerprint"
              />
            )}
          </form.AppField>

          <form.AppField
            name="operator"
            validators={{
              onBlur: ({ value }) => validatePartyId(value),
              onChange: ({ value }) => validatePartyId(value),
            }}
          >
            {field => (
              <field.TextField
                title="Synchronizer Operator"
                id="register-synchronizer-operator"
                subtitle="The party that operates the synchronizer and observes its traffic purchases"
              />
            )}
          </form.AppField>

          <form.AppField
            name="discountFactor"
            validators={{
              onBlur: ({ value }) => validateDiscountFactor(value.trim()),
              onChange: ({ value }) => validateDiscountFactor(value.trim()),
            }}
          >
            {field => (
              <field.TextField
                title="Traffic Discount"
                id="register-synchronizer-discount-factor"
                subtitle="Multiplies this synchronizer's traffic price, greater than 0 and at most 1. 1 registers at the full price."
              />
            )}
          </form.AppField>

          <form.AppField
            name="outageAdvance"
            validators={{
              onBlur: ({ value }) => validateOutageAdvance(value.trim()),
              onChange: ({ value }) => validateOutageAdvance(value.trim()),
            }}
          >
            {field => (
              <field.TextField
                title="Outage Traffic Advance"
                id="register-synchronizer-outage-advance"
                subtitle="Bytes each member may use beyond what it has bought while the operator is cut off from the global synchronizer, taken back when it reconnects. 0 means no advance."
              />
            )}
          </form.AppField>

          <form.AppField
            name="expiryDate"
            validators={{
              onChange: ({ value }) => validateExpiration(value),
              onBlur: ({ value }) => validateExpiration(value),
            }}
          >
            {field => (
              <field.DateField
                title={CREATE_PROPOSAL_LABEL_THRESHOLD_DEADLINE}
                description={THRESHOLD_DEADLINE_SUBTITLE}
                id="register-synchronizer-expiry-date"
              />
            )}
          </form.AppField>

          <form.AppField
            name="effectiveDate"
            validators={{
              onChange: ({ value }) => validateEffectiveDate(value),
              onBlur: ({ value }) => validateEffectiveDate(value),
            }}
            children={_ => (
              <EffectiveDateField
                title={CREATE_PROPOSAL_LABEL_EFFECTIVE_AT}
                initialEffectiveDate={initialEffectiveDate.format(dateTimeFormatISO)}
                id="register-synchronizer-effective-date"
              />
            )}
          />

          <form.AppField
            name="summary"
            validators={{
              onBlur: ({ value }) => validateSummary(value),
              onChange: ({ value }) => validateSummary(value),
            }}
          >
            {field => (
              <field.ProposalSummaryField
                id="register-synchronizer-summary"
                title={CREATE_PROPOSAL_LABEL_PROPOSAL_SUMMARY}
              />
            )}
          </form.AppField>

          <form.AppField
            name="url"
            validators={{
              onBlur: ({ value }) => validateUrl(value),
              onChange: ({ value }) => validateUrl(value),
            }}
          >
            {field => (
              <field.TextField
                title={CREATE_PROPOSAL_LABEL_SUPPORTING_URL}
                id="register-synchronizer-url"
                muiTextFieldProps={{ placeholder: SUPPORTING_URL_PLACEHOLDER }}
              />
            )}
          </form.AppField>
        </>
      )}

      <form.AppForm>
        <ProposalSubmissionError error={mutation.error} />

        <form.FormErrors />

        <form.FormControls
          showConfirmation={showConfirmation}
          onEdit={() => setShowConfirmation(false)}
        />
      </form.AppForm>
    </FormLayout>
  );
};
