// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.store.db

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext, SynchronizerAlias}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.topology.{Member, PartyId, SynchronizerId}
import org.lfdecentralizedtrust.splice.codegen.java.splice.decentralizedsynchronizer.MemberTraffic
import org.lfdecentralizedtrust.splice.config.IngestionConfig
import org.lfdecentralizedtrust.splice.environment.{DarResources, RetryProvider}
import org.lfdecentralizedtrust.splice.store.{HardLimit, Limit, StoreTestBase}
import org.lfdecentralizedtrust.splice.syncoperator.store.SyncOperatorStore
import org.lfdecentralizedtrust.splice.syncoperator.store.db.DbSyncOperatorStore
import org.lfdecentralizedtrust.splice.util.{Contract, ResourceTemplateDecoder, TemplateJsonDecoder}

import scala.concurrent.Future
import scala.jdk.OptionConverters.*

abstract class SyncOperatorStoreTest extends StoreTestBase with HasExecutionContext {

  protected val operatorParty: PartyId = providerParty(1)
  protected val otherOperator: PartyId = providerParty(2)
  protected val ourSynchronizer: SynchronizerId =
    SynchronizerId.tryFromString("dedicated::operator")
  protected val foreignSynchronizer: SynchronizerId =
    SynchronizerId.tryFromString("dedicated::someone-else")

  protected val alice: Member = mkParticipantId("alice")
  protected val bob: Member = mkParticipantId("bob")

  protected def mkStore(): Future[SyncOperatorStore]

  private def memberTraffic(
      member: Member,
      totalPurchased: Long,
      synchronizerId: SynchronizerId = ourSynchronizer,
      operator: Option[PartyId] = Some(operatorParty),
      migrationId: Long = 0L,
  ): Contract[MemberTraffic.ContractId, MemberTraffic] =
    memberTrafficFor(member.toProtoPrimitive, totalPurchased, synchronizerId, operator, migrationId)

  private def memberTrafficFor(
      memberId: String,
      totalPurchased: Long,
      synchronizerId: SynchronizerId,
      operator: Option[PartyId],
      migrationId: Long,
  ): Contract[MemberTraffic.ContractId, MemberTraffic] = {
    val template = new MemberTraffic(
      dsoParty.toProtoPrimitive,
      memberId,
      synchronizerId.toProtoPrimitive,
      migrationId,
      totalPurchased,
      1L,
      java.math.BigDecimal.ONE,
      java.math.BigDecimal.ONE,
      operator.map(_.toProtoPrimitive).toJava,
    )
    contract(
      MemberTraffic.TEMPLATE_ID_WITH_PACKAGE_ID,
      new MemberTraffic.ContractId(nextCid()),
      template,
    )
  }

  private def ingest(
      store: SyncOperatorStore,
      contracts: Seq[Contract[MemberTraffic.ContractId, MemberTraffic]],
  ): Future[Unit] =
    store.multiDomainAcsStore.testIngestionSink.ingestAcs(
      nextOffset(),
      // the purchase is made and recorded on the decentralized synchronizer, not on the dedicated
      // one it names
      contracts.zipWithIndex.map { case (c, i) =>
        // mirrors `observer (optionalToList operator)` on the template
        val observers = c.payload.operator.toScala.map(PartyId.tryFromProtoPrimitive).toList
        toActiveContract(dummyDomain, c, i.toLong, observers)
      },
      Seq.empty,
      Seq.empty,
    )

  "SyncOperatorStore" should {

    "sum the purchases made for this synchronizer, per member" in {
      for {
        store <- mkStore()
        _ <- ingest(
          store,
          Seq(memberTraffic(alice, 100L), memberTraffic(alice, 250L), memberTraffic(bob, 700L)),
        )
        byMember <- store.getPurchasedTrafficByMember()
      } yield byMember shouldBe Map(alice -> 350L, bob -> 700L)
    }

    "ignore purchases for another synchronizer" in {
      for {
        store <- mkStore()
        _ <- ingest(store, Seq(memberTraffic(alice, 100L, synchronizerId = foreignSynchronizer)))
        byMember <- store.getPurchasedTrafficByMember()
      } yield byMember shouldBe empty
    }

    "ignore purchases observed by another operator" in {
      for {
        store <- mkStore()
        _ <- ingest(store, Seq(memberTraffic(alice, 100L, operator = Some(otherOperator))))
        byMember <- store.getPurchasedTrafficByMember()
      } yield byMember shouldBe empty
    }

    "ignore purchases with no operator, which are decentralized-synchronizer traffic" in {
      for {
        store <- mkStore()
        _ <- ingest(store, Seq(memberTraffic(alice, 100L, operator = None)))
        byMember <- store.getPurchasedTrafficByMember()
      } yield byMember shouldBe empty
    }

    // A registered synchronizer is pinned to migration id 0, so anything else is not ours even if
    // it names our synchronizer and operator.
    "ignore purchases carrying a non-zero migration id" in {
      for {
        store <- mkStore()
        _ <- ingest(store, Seq(memberTraffic(alice, 100L, migrationId = 1L)))
        byMember <- store.getPurchasedTrafficByMember()
      } yield byMember shouldBe empty
    }

    // A member id is plain Text on-ledger, so a purchase can name one that does not parse. Ingestion
    // records such a contract with no member, and this query has to leave it out.
    "skip a purchase whose member id does not parse" in {
      for {
        store <- mkStore()
        _ <- ingest(
          store,
          Seq(
            memberTraffic(alice, 100L),
            memberTrafficFor(
              memberId = "not-a-member-id",
              totalPurchased = 700L,
              synchronizerId = ourSynchronizer,
              operator = Some(operatorParty),
              migrationId = 0L,
            ),
          ),
        )
        byMember <- store.getPurchasedTrafficByMember()
      } yield byMember shouldBe Map(alice -> 100L)
    }

    "report no members when nothing has been purchased" in {
      for {
        store <- mkStore()
        // queries wait on ACS ingestion, so the empty ACS still has to be ingested
        _ <- ingest(store, Seq.empty)
        byMember <- store.getPurchasedTrafficByMember()
      } yield byMember shouldBe empty
    }
  }
}

class DbSyncOperatorStoreTest
    extends SyncOperatorStoreTest
    with HasActorSystem
    with SplicePostgresTest
    with AcsJdbcTypes
    with AcsTables {

  override protected def mkStore(): Future[SyncOperatorStore] = {
    val packageSignatures =
      ResourceTemplateDecoder.loadPackageSignaturesFromResources(DarResources.amulet.all)
    implicit val templateJsonDecoder: TemplateJsonDecoder =
      new ResourceTemplateDecoder(packageSignatures, loggerFactory)

    val store = new DbSyncOperatorStore(
      SyncOperatorStore.Key(operatorParty, dsoParty, ourSynchronizer),
      storage,
      loggerFactory,
      RetryProvider(loggerFactory, timeouts, FutureSupervisor.Noop, NoOpMetricsFactory),
      domainMigrationId,
      participantId = mkParticipantId("SyncOperatorStoreTest"),
      IngestionConfig(),
      defaultLimit = HardLimit.tryCreate(Limit.DefaultMaxPageSize),
    )(parallelExecutionContext, implicitly, implicitly)
    for {
      _ <- store.multiDomainAcsStore.testIngestionSink.initialize()
      _ <- store.domains.ingestionSink.ingestConnectedDomains(
        Map(SynchronizerAlias.tryCreate(dummyDomain.toProtoPrimitive) -> dummyDomain)
      )
    } yield store
  }

  override protected def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[?] = resetAllAppTables(storage)
}
